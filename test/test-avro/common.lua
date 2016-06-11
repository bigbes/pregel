local ffi = require('ffi')
local fio = require('fio')
local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local clock = require('clock')
local fiber = require('fiber')
local digest = require('digest')

local pmaster = require('pregel.master')
local pworker = require('pregel.worker')
local avro      = require('pregel.avro')
local xpcall_tb = require('pregel.utils').xpcall_tb

local algo         = require('algo')
local avro_loaders = require('avro_loaders')

--[[------------------------------------------------------------------------]]--
--[[--------------------------------- Utils --------------------------------]]--
--[[------------------------------------------------------------------------]]--

local function math_round(fnum)
    return (fnum % 1 >= 0.5) and math.ceil(fnum) or math.floor(fnum)
end

--[[------------------------------------------------------------------------]]--
--[[--------------------------- Job configuration --------------------------]]--
--[[------------------------------------------------------------------------]]--

-- common constants
ffi.cdef[[
    struct gd_task_type {
        static const int MASTER = 0x00;
        static const int TASK   = 0x01;
        static const int DATA   = 0x02;
    };
    struct gd_message_command {
        static const int NONE                = 0x00;
        static const int FETCH               = 0x01;
        static const int PREDICT_CALIBRATION = 0x02;
        static const int PREDICT             = 0x03;
        static const int TERMINATE           = 0x04;
    };
    struct gd_node_status {
        static const int UNKNOWN  = 0x00;
        static const int NEW      = 0x01;
        static const int WORKING  = 0x02;
        static const int INACTIVE = 0x03;
    };
    struct gd_task_phase {
        static const int SELECTION   = 0x00;
        static const int TRAINING    = 0x01;
        static const int CALIBRATION = 0x02;
        static const int PREDICTION  = 0x03;
        static const int DONE        = 0x04;
    };
]]
local task_type       = ffi.new('struct gd_task_type')
local message_command = ffi.new('struct gd_message_command')
local node_status     = ffi.new('struct gd_node_status')
local task_phase      = ffi.new('struct gd_task_phase')
-- keys of dataSet
local dataSetKeys            = {'vid', 'email', 'okid', 'vkid'}
-- config keys
local FEATURES_LIST          = "features.list"
local TASKS_CONFIG_HDFS_PATH = "tasks.config.hdfs.path"
local DATASET_PATH           = '/Users/blikh/src/work/pregel-data/tarantool-test'

-- other
local SUFFIX_TRAIN           = "train"
local SUFFIX_TEST            = "test"
local DISTRIBUTED_GD_GROUP   = "Distributed GD"
local MISSING_USERS_COUNT    = "Missing users"
local MASTER_VERTEX_TYPE     = "MASTER"
local TASK_VERTEX_TYPE       = "TASK"
-- Parameters of gradient descend / algorithm
local GDParams = nil
do
    local __params = {
        ['max.dataset.size']               = 30000,
        ['max.gd.iter']                    = 300,
        ['gd.loss.average.factor']         = 0.2,
        ['gd.loss.convergence.factor']     = 1e-4,
        ['train.batch.size']               = 500,
        ['test.batch.size']                = 300,
        ['negative.vertices.fraction']     = 0.05,
        ['n.calibration.vertices']         = 0.2,
        ['p.report.prediction']            = 10000,
        -- this should be a divisor of 0
        ['calibration.bucket.percents']    = 5.0,
        ['max.predicted.calibrated.value'] = 1000
    }
    GDParams = setmetatable({}, {
        __index = function(self, key)
            local value = __params[key]
            if value == nil then
                error(string.format('undefined constant "%s"', tostring(key)))
            end
            return value
        end,
        __newindex = function(self, key, val)
            error('trying to modify read-only table')
        end
    })
end

--[[------------------------------------------------------------------------]]--
--[[----------------------------- Worker Context ---------------------------]]--
--[[------------------------------------------------------------------------]]--

local wc = nil
do
    local randomVertexIds       = {} -- List of vertices
    local taskPhases            = {} -- taskName -> phase
    local taskDeploymentConfigs = {} -- taskName -> config
    local taskDataSet           = {} -- taskName -> data_set_path
    local jobID                 = math.random(0, math.pow(2, 16))

    local predictionReportSamplingProb = GDParams['p.report.prediction']
    local calibrationBucketPercents    = GDParams['calibration.bucket.percents']

    local fd = io.open(fio.pathjoin(DATASET_PATH, 'express_prediction_config.json'))
    assert(fd)
    local input = json.decode(fd:read('*a'))
    fd:close()

    for _, task_config in ipairs(input) do
        local name  = task_config['name']
        local input = task_config['input']
        for _, deployment in ipairs(task_config['deployment']) do
            local key = table.concat({name, deployment['user_id_type']}, ':')
            taskDeploymentConfigs[key] = deployment
        end
    end

    local dataSetKeysTypes = {
        {'email',    'string'},
        {'okid',     'string'},
        {'vkid',     'string'},
        {'category', 'int'   }
    }

    local function processDataSet(input)
        local output = fun.iter(dataSetKeysTypes):map(function(field)
            local fname = field[1]
            local ftype = field[2]
            if input[fname] ~= nil then
                return fname, input[fname][ftype]
            end
        end):totable()
        local vid = input['vid']
        if vid and #vid > 0 then
            output['vid'] = vid
        end
        return output
    end

    wc = setmetatable({
        randomVertexIds              = randomVertexIds,
        taskPhases                   = taskPhases,
        taskDeploymentConfigs        = taskDeploymentConfigs,
        taskDataSet                  = taskDataSet,
        jobID                        = jobID,
        calibrationBucketPercents    = calibrationBucketPercents,
        predictionReportSamplingProb = predictionReportSamplingProb,
    }, {
        __index = {
            addRandomVertexId  = function(self, vertexId)
                table.insert(self.randomVertexIds, vertexId)
            end,
            iterateRandomVertexIds = function(self)
                return ipairs(self.randomVertexIds)
            end,
            setTaskPhase = function(self, name, phase)
                self.taskPhases[name] = phase
            end,
            getTaskPhase = function(self, name)
                return self.taskPhases[name] or task_phase.UNKNOWN
            end,
            iterateDataSet = function(self, name)
                local n    = 0
                local file = fio.open(self.taskDataSet[name])
                local line = ''
                local function iterator()
                    while true do
                        if line:find('\n') == nil then
                            local rv = file:read(65536)
                            if rv == nil then
                                assert(false)
                                return nil
                            end
                            line = line .. rv
                        else
                            local input, line = line:match('([^\n]*)\n(.*)')
                            input = json.decode(input)
                            n = n + 1
                            return n, processDataSet(input)
                        end
                    end
                end
                return iterator, nil
            end
        }
    })
end

local node_common_methods = {
    get_status = function(self)
        return self:get_value().status
    end,
    set_status = function(self, status)
        local v = self:get_value()
        v.status = status
        self:set_value(v)
    end,
    compute_new = function(self)
        self:__init()
        local status = self:get_status()
        if status == node_status.WORKING then
            self:work()
        elseif status == node_status.INACTIVE then
            log.debug('Vertex %s is inactive: doing nothing', self:get_name())
            self:vote_halt()
        else
            assert(false, string.format(
                'Unexpected data vertex status: %s - %d',
                self:get_name(), status
            ))
        end
    end,
}

local node_master_methods = {
    __init = function(self)
        self:set_status(node_status.WORKING)
    end,
    work = function(self)
        local taskName = self:get_value().name

        if self:get_superstep() == 1 then
            local val = self:get_value()
            for name, _ in pairs(wc.taskInputs) do
                self:add_vertex({
                    name     = taskName,
                    vtype    = task_type.VERTEX,
                    features = val.features,
                    status   = node_status.NEW
                })
            end
        end
        self:set_status(node_status.INACTIVE)
        self:vote_halt()
    end,
}

local node_task_methods = {
    __init = function(self)
        if self:get_status() == node_status.NEW then
            self:set_status(node_status.WORKING)
        end
    end,
    work = function(self)
        local value = self:get_value()
        local taskName = self:get_value().name
        local phase = self:get_phase()

        if false then
            assert(false)
        elseif phase == task_phase.SELECTION then
            for task in self:iterateDataSet() do
                local ktype, kname, value = unpack(task)
                local name = ('%s:%s'):format(ktype, kname)
                self:send_message(name, {
                    sender   = self:get_name(),
                    command  = message_command.FETCH,
                    target   = value,
                    features = {}
                })
            end

            wc:set_phase(task_phase.TRAINING)
        elseif phase == task_phase.TRAINING then
            local dsLocalPathPrefix = fio.abspath(
                string.format('%s_%s', wc.jobID, taskName)
            )
            log.info('setting data set LOCAL_PATH for task %s to %s',
                     taskName, dsLocalPathPrefix)

            local testVerticesFraction = GDParams['test.vertices.fraction']
            log.info('Test vertices fraction %f', testVerticesFraction)

            local n, recordCounts = self:saveDataSetToLocalTempFiles(
                taskName, dsLocalPathPrefix, testVerticesFraction
            )
            if n == 0 then
                log.warn('Master didn\'t receive any messages, so no training occured')
                log.warn('Waiting one superstep')
                return
            end

            local d = #value.features
            local trainBatchSize = GDParams['train.batch.size']
            local testBatchSize  = GDParams['test.batch.size']
            local maxIter        = GDParams['max.gd.iter']
            local alpha          = GDParams['gd.loss.average.factor']
            local epsilon        = GDParams['gd.loss.convergence.factor']

            local params = self:train(taskName, recordCounts, d, maxIter,
                                      trainBatchSize, testBatchSize, alpha,
                                      epsilon)
            value.features = params
            self:set_value(value)

            local nCalibrationMessages = GDParams['n.calibration.vertices']
            log.info('Number of calibration messages: %d', nCalibrationMessages)

            local calibrationProb = math.min(
                1.0 * nCalibrationMessages / #wc.RandomVertexIDS,
                1.0
            )
            log.info('Probability to take message for calibration: %f',
                     calibrationProb)

            local n = 0
            for idx, randomID in wc:iterateRandomVertexIds() do
                if math.random() < calibrationProb then
                    self:send_message(randomID, {
                        sender   = self:get_name(),
                        command  = message_command.COMMAND_PREDICT_CALIBRATION,
                        target   = 0.0,
                        features = value.features,
                    })
                    n = n + 1
                end
            end
            log.info('sent %d calibration messages. Waiting for response')

            wc:set_phase(task_phase.CALIBRATION)
        elseif phase == task_phase.CALIBRATION then
            local ds = algo.PercentileCounter()
            for _, msg, _ in self:pairs_messages() do
                ds.addValue(msg.target)
            end
            if ds:getN() == 0 then
                log.warn('Master didn\'t receive any messages, so no calibration occured')
                log.warn('Waiting one superstep')
                return
            end
            local cbp = GDParams['calibration.buckets.percents']
            log.info('Calibration bucket percents %f', cbp)
            local parametersAndCalibration = self:calibrate(value.features, ds,
                                                            #value.features, cbp)
            local broadcast = {
                sender   = self:get_name(),
                command  = message_command.PREDICT,
                target   = 0.0,
                features = parametersAndCalibration
            }
            log.info('<%s> Set aggregator to broadcast model across all vertices to %s',
                     taskName, json.encode(broadcast))
            self:set_aggregation(taskName, broadcast)

            wc:set_phase(task_phase.PREDICTION)
        elseif phase == task_phase.PREDICTION then
            log.info("Calibrated data:")
            local n = 1
            for _, msg, _ in self:pairs_messages() do
                log.info('%d> %f', n, msg.target)
                n = n + 1
            end
            if n == 0 then
                log.info('Master didn\'t receive any messaged, so no prediction occured')
                log.info('Waiting one superstep')
                return
            end

            wc:set_phase(task_phase.DONE)
        elseif phase == task_phase.DONE then
            self:vote_halt()
            self:set_status(node_status.INACTIVE)
        else
            assert(false)
        end
    end,
    calibrate = function(self, parameters, ds, dim, calibrationBucketPercents)
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1;
        log.info('Model dimensionality: %d, calibration bucket percents: %f',
                 dim, calibrationBucketPercents)
        log.info('Number of calibration percentiles: %d', nPercentiles)
        local parametersWithCalibration = fun.iter(parameters):totable()
        for p = 1, nPercentiles do
            table.insert(parametersWithCalibration, ds.getPercentile(
                (p + 1) * calibrationBucketPercents
            ))
        end
        return parametersWithCalibration
    end,
    train = function(self, taskName, recordCounts, dim, maxIter, trainBatchSize,
                     testBatchSize, alpha, epsilon)
        log.info('Initializing Gradient descent for task %s', taskName)
        log.info(' - train batch size %d', trainBatchSize)
        log.info(' - test batch size %d', testBatchSize)
        log.info(' - maximum number of iteration %d', maxIter)
        log.info(' - loss averaging factor %f', alpha)
        log.info(' - loss convergence factor %f', epsilon)

        local gd = algo.GradientDescent('hinge', 'l2')
        local parameters = gd:initialize(dim)
        log.info('initialized model parameters to %s', json.encode(parameters))

        local readerMap = {}
        local stat, err = pcall(function()
            for dsLocalPath, recordCount in pairs(recordCounts) do
                if recordCount > 0 then
                    -- TODO: port line 343
                else
                    error(
                        string.format('No records in file %s. Failing the task',
                                      dsLocalPath)
                    )
                end
                -- TODO: port lines 348-409
            end
        end)
        for _, dsLocalPath in ipairs(recordCounts) do
            self:removeDataSetLocalTempFile(taskName, dsLocalPath)
        end
        if stat == false then
            error(err)
        end

        log.info('Finihsed GD, new parameters: %s', json.encode(parameters))
        return parameters
    end,
    saveDataSetToLocalTempFiles = function(self, taskName, dsLocalPathPrefix,
                                           testVerticesFraction)
        log.info('<%s> Saving data to temp files using prefix %s',
                 taskName, dsLocalPathPrefix)

        local writerMap = {}
        local recordCounts = {}

        for _, msg in self:pairs_messages() do
            local target = math_round(msg.target)
            local suffix = math.random() < testVerticesFraction and SUFFIX_TEST or SUFFIX_TRAIN
            local dsLocalPath = ('%s_%d_%s'):format(dsLocalPathPrefix, target, suffix)

            if writerMap[dsLocalPath] == nil then
                writerMap[dsLocalPath] = fio.open(dsLocalPath, {'O_WRONLY'})
                assert(writerMap[dsLocalPath] ~= nil)
                recordCounts[dsLocalPath] = 0
            end
            local fd = writerMap[dsLocalPath]
            fd:write(self:datumToJson(msg.target, msg.features))
            fd:write('\n')
            recordCounts[dsLocalPath] = recordCounts[dsLocalPath] + 1
        end
        for dsLocalPath, fd in pairs(recordCounts) do
            log.info('<%s> Written data set file: %s -> %d',
                     taskName, dsLocalPath, recordCounts[dsLocalPath])
            fd:close()
        end

        return recordCounts
    end,
    removeDataSetLocalTempFiles = function(self, taskName, dsLocalPath)
        log.info('<%s> Removing temporary file %s', taskName, dsLocalPath)
        fio.delete(dsLocalPath)
        log.info('<%s> Removed temporary file %s', taskName, dsLocalPath)
    end,
    jsonToDatum = function(self, line)
        return unpack(json.decode(line))
    end,
    datumToJson = function(self, target, features)
        local obj = json.encode{target, features}
    end,
    iterateDataSet = function(self, name)
        local dataSetKeys = {
            'vid', 'email', 'okid', 'vkid',
        }
        local iter_func = wc:iterateDataSet(name)
        local last_item = {}
        local category  = 1
        local iterator = function()
            if last_item[1] == nil then
                last_item = iter_func()
                if last_item == nil then
                    return
                end
                category = last_item['category']
                last_item = fun.iter(dataSetKeys):map(function(key)
                    if last_item[key] ~= nil then
                        return { key, last_item[key], category }
                    end
                end):totable()
            end
            return table.remove(last_item)
        end
        return iterator, nil
    end,
}

local crc32 = digest.crc32.new()

local node_data_methods = {
    __init = function(self)
        local status = self:get_status()
        local negativeVerticesFraction = GDParams['negative.verices.fraction']
        if status == node_status.STATUS_NEW then
            local hash = digest.crc32(self:get_key()) % 1000
            if hash <= 1000 * negativeVerticesFraction then
                wc:addRandomVertex(self:get_name())
            end
            self:set_status(node_status.STATUS_WORKING)
        elseif status == node_status.STATUS_UNKNOWN then
            self:set_status(node_status.STATUS_INACTIVE)
        end
    end,
    work = function(self)
        for _, msg in self:pairs_messages() do
            -- return feature vector to master
            if msg.command == message_command.FETCH then
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = msg.target,
                    features = self:get_features()
                })
            -- compute raw prediction and return to master
            elseif msg.command == message_command.PREDICT_CALIBRATION then
                local prediction = self:predictRaw(msg.features)
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = prediction,
                    features = {}
                })
            else
                assert(false)
            end
        end

        local isPredictionPhase = false
        local predictions = {}

        -- do calibrated prediction for each task
        for taskName, _ in pairs(wc.taskDataSet) do
            local msg = self:get_aggregation(taskName)

            -- predict and save
            if msg.command == message_command.PREDICT then
                isPredictionPhase = true
                local calibratedPrediction = self:predictCalibrated(
                    msg.features,
                    wc.calibrationBucketPercent
                )

                -- return to master for report, maybe
                if math.random() < wc.predictionReportSamplingProb then
                    self:send_message(msg.sender, {
                        sender   = self:get_name(),
                        command  = message_command.NONE,
                        target   = calibratedPrediction,
                        features = {}
                    })
                end

                -- write (audience, score) pair if threshold exceeded
                local dcName = ('%s:%s'):format(taskName, self.idType)
                local dc = wc.taskDeploymentConfigs[dcName]
                if (dc ~= nil and
                    calibratedPrediction >
                        (GDParams['max.predicted.calibrated.value'] *
                         (1 - dc.threshold))) then
                    table.insert(predictions, dc.targeting)
                    table.insert(predictions, calibratedPrediction)
                end
            end
        end

        if isPredictionPhase then
            local val = self:get_value()
            val.features = predictions
            self:set_value(val)
            self:set_status(node_status.INACTIVE)
            self:vote_halt()
        end
    end,
    predictRaw = function(self, param)
        local prediction = 0.0
        local features = self.get_value().features
        for i, feature in ipairs(features) do
            prediction = prediction + feature * param[i]
        end
        return prediction
    end,
    predictCalibrated = function(self, param, calibrationBucketPercents)
        local features = self.get_value().features
        local dim = #features
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1
        local percentileStep = GDParams['max.predicted.calibrated.value'] / 100
              percentileStep = math.floor(percentileStep * calibrationBucketPercents)
        local prediction = self:predictRaw(param)

        local calibratedPrediction = -1
        for i = 0, nPercentiles - 1 do
            if prediction < param[dim + i] then
                calibratedPrediction = math.random(-1000000, 1000000) + i * percentileStep
                break
            end
        end
        if calibratedPrediction == -1 then
            calibratedPrediction = math.random(-1000000, 1000000)
            calibratedPrediction = calibratedPrediction + nPercentiles * percentileStep
        end
        return calibratedPrediction
    end
}

--[[------------------------------------------------------------------------]]--
--[[------------------------ Configuration of Runner -----------------------]]--
--[[------------------------------------------------------------------------]]--

local vertex_mt      = nil
local node_master_mt = nil
local node_task_mt   = nil
local node_data_mt   = nil

local function computeGradientDescent(vertex)
    if vertex_mt == nil then
        vertex_mt = getmetatable(vertex)
        node_master_mt = {
            __index = {}
        }
        node_task_mt = {
            __index = {}
        }
        node_data_mt = {
            __index = {}
        }
        for k, v in pairs(node_common_methods) do
            node_master_mt.__index[k] = v
            node_task_mt.__index[k] = v
            node_data_mt.__index[k] = v
        end
        for k, v in pairs(node_master_methods) do
            node_master_mt.__index[k] = v
        end
        for k, v in pairs(node_task_methods) do
            node_task_mt.__index[k] = v
        end
        for k, v in pairs(node_data_methods) do
            node_data_mt.__index[k] = v
        end
    end

    local vtype = vertex:get_value().vtype

    if vtype == task_type.MASTER then
        setmetatable(vertex, node_master_mt)
    elseif vtype == task_type.TASK then
        if vertex:get_superstep() == 0 then
            return
        end
        setmetatable(vertex, node_task_mt)
    else
        setmetatable(vertex, node_data_mt)
    end
    vertex:compute_new()
    setmetatable(vertex, vertex_mt)
end

local function obtain_type(name)
    return string.match('(%a+):(%w*)')
end

local function obtain_name(value)
    if value.vtype == MASTER_VERTEX_TYPE then
        return 'MASTER:'
    elseif value.vtype == TASK_VERTEX_TYPE then
        return ('%s:%s'):format(TASK_VERTEX_TYPE, value.name)
    end
    for _, name in ipairs(dataSetKeys) do
        local key_value = value.key[name]
        if key_value ~= nil and
           type(key_value) == 'string' and
           #key_value > 0 then
            return ('%s:%s'):format(name, key_value)
        end
    end
    assert(false)
end

local worker, port_offset = arg[0]:match('(%a+)-(%d+)')
port_offset = port_offset or 0

local common_cfg = {
    master         = 'localhost:3301',
    workers        = {
        'localhost:3302',
        'localhost:3303',
        'localhost:3304',
        'localhost:3305',
    },
    compute        = computeGradientDescent,
    combiner       = nil,
    master_preload = avro_loaders.master,
    worker_preload = avro_loaders.worker,
    preload_args   = DATASET_PATH,
    squash_only    = false,
    pool_size      = 250,
    delayed_push   = false,
    obtain_name    = obtain_name
}

if worker == 'worker' then
    box.cfg{
        wal_mode = 'none',
        slab_alloc_arena = 1,
        listen = 'localhost:' .. tostring(3301 + port_offset),
        background = true,
        logger_nonblock = false
    }
else
    box.cfg{
        wal_mode = 'none',
        listen = 'localhost:' .. tostring(3301 + port_offset),
        logger_nonblock = false
    }
end

box.once('bootstrap', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

if worker == 'worker' then
    worker = pworker.new('test', common_cfg)
else
    xpcall_tb(function()
        local master = pmaster.new('test', common_cfg)
        master:wait_up()
        if arg[1] == 'load' then
            -- master:preload()
            master:preload_on_workers()
            -- master.mpool:send_wait('snapshot')
        end
        -- master:start()
    end)
    os.exit(0)
end
