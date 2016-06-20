local ffi = require('ffi')
local fio = require('fio')
local fun = require('fun')
local log = require('log')
local json = require('json')
local yaml = require('yaml')
local clock = require('clock')
local errno = require('errno')
local fiber = require('fiber')
local digest = require('digest')

local pmaster   = require('pregel.master')
local pworker   = require('pregel.worker')
local avro      = require('pregel.avro')

local xpcall_tb = require('pregel.utils').xpcall_tb
local deepcopy  = require('pregel.utils.copy').deep

local algo         = require('algo')
local avro_loaders = require('avro_loaders')
local constants    = require('constants')
local utils        = require('utils')

local worker, port_offset = arg[0]:match('(%a+)-(%d+)')
port_offset = port_offset or 0

if worker == 'worker' then
    box.cfg{
        wal_mode           = 'none',
        slab_alloc_arena   = 10,
        -- slab_alloc_maximal = 4*1024*1024,
        listen             = '0.0.0.0:' .. tostring(3301 + port_offset),
        background         = true,
        logger_nonblock    = true
    }
else
    box.cfg{
        wal_mode           = 'none',
        listen             = '0.0.0.0:' .. tostring(3301 + port_offset),
        logger_nonblock    = true
    }
end

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {
    if_not_exists = true
})

--[[------------------------------------------------------------------------]]--
--[[--------------------------------- Utils --------------------------------]]--
--[[------------------------------------------------------------------------]]--

local math_round   = utils.math_round
local log_features = utils.log_features

local NULL = json.NULL

--[[------------------------------------------------------------------------]]--
--[[--------------------------- Job configuration --------------------------]]--
--[[------------------------------------------------------------------------]]--

local vertex_type            = constants.vertex_type
local message_command        = constants.message_command
local node_status            = constants.node_status
local task_phase             = constants.task_phase
-- keys of dataSet
local dataSetKeys            = constants.dataSetKeys
-- config keys
local FEATURES_LIST          = constants.FEATURES_LIST
local TASKS_CONFIG_HDFS_PATH = constants.TASKS_CONFIG_HDFS_PATH
local DATASET_PATH           = constants.DATASET_PATH

-- other
local SUFFIX_TRAIN           = constants.SUFFIX_TRAIN
local SUFFIX_TEST            = constants.SUFFIX_TEST
local DISTRIBUTED_GD_GROUP   = constants.DISTRIBUTED_GD_GROUP
local MISSING_USERS_COUNT    = constants.MISSING_USERS_COUNT
local MASTER_VERTEX_TYPE     = constants.MASTER_VERTEX_TYPE
local TASK_VERTEX_TYPE       = constants.TASK_VERTEX_TYPE
-- Parameters of gradient descend / algorithm
local GDParams               = constants.GDParams

--[[------------------------------------------------------------------------]]--
--[[----------------------------- Worker Context ---------------------------]]--
--[[------------------------------------------------------------------------]]--

local wc = nil
do
    local featureList           = {}
    local featureMap            = {}
    local randomVertexIds       = {} -- List of vertices
    local taskPhases            = {} -- taskName -> phase
    local taskDeploymentConfigs = {} -- taskName:uid_type -> config
    local taskDataSet           = {} -- taskName -> data_set_path
    local jobID                 = math.random(0, math.pow(2, 16))

    local predictionReportSamplingProb = GDParams['p.report.prediction']
    local calibrationBucketPercents    = GDParams['calibration.bucket.percents']

    -- Open/Parse file with features
    local fd = io.open(fio.pathjoin(DATASET_PATH, 'features.txt'))
    assert(fd, "Can't open file for reading")
    local line, input = fd:read('*a'), nil
    assert(line, "Bad input")
    local n = 1
    while true do
        if line:match('\n') ~= nil then
            input, line = line:match('([^\n]+)\n(.*)')
        elseif #line > 0 then
            input = line
            line = ''
        else
            break
        end
        table.insert(featureList, input)
        featureMap[input] = n
        n = n + 1
    end
    log.info("<worker_context> Found %d features", #featureList)
    fd:close()

    local fd = io.open(fio.pathjoin(DATASET_PATH, 'express_prediction_config.json'))
    assert(fd, "Can't open file for reading")
    local input = json.decode(fd:read('*a'))
    assert(input, "Bad input")
    fd:close()

    local function check_file(fname)
        local file = fio.open(fname, {'O_RDONLY'})
        if file == nil then
            local errstr = "Can't open file '%s' for reading: %s [errno %d]"
            errstr = string.format(errstr, fname, errno.strerror(), errno())
            error(errstr)
        end
        file:close()
        return fname
    end

    for _, task_config in ipairs(input) do
        local name  = task_config['name']
        local input = task_config['input']
        for _, deployment in ipairs(task_config['deployment']) do
            local key = ('%s:%s'):format(name, deployment['user_id_type'])
            taskDeploymentConfigs[key] = deployment
        end
        taskPhases[name] = task_phase.SELECTION
        log.info("<worker_context> Added config for '%s'", name)
    end

    local dataSetKeysTypes = {'email', 'okid', 'vkid'}

    wc = setmetatable({
        featureList                  = featureList,
        featureMap                   = featureMap,
        randomVertexIds              = randomVertexIds,
        taskPhases                   = taskPhases,
        taskDeploymentConfigs        = taskDeploymentConfigs,
        taskDataSet                  = taskDataSet,
        jobID                        = jobID,
        calibrationBucketPercents    = calibrationBucketPercents,
        predictionReportSamplingProb = predictionReportSamplingProb,
    }, {
        __index = {
            addRandomVertex = function(self, vertexId)
                table.insert(self.randomVertexIds, vertexId)
            end,
            iterateRandomVertexIds = function(self)
                return ipairs(self.randomVertexIds)
            end,
            setTaskPhase = function(self, name, phase)
                self.taskPhases[name] = phase
            end,
            getTaskPhase = function(self, name)
                return self.taskPhases[name] or task_phase.SELECTION
            end,
            iterateDataSet = function(self, name)
                local function processDataSet(input)
                    local category = input.category and input.category.int
                    local output = fun.iter(dataSetKeysTypes):map(function(fname)
                        if input[fname] ~= nil then
                            return {fname, input[fname].string, category}
                        end
                    end):totable()
                    local vid = input['vid']
                    if vid and #vid > 0 then
                        table.insert(output, {'vid', vid, category})
                    end
                    return output
                end

                local file  = fio.open(self.taskDataSet[name][1], {'O_RDONLY'})
                local line  = ''
                local errstr = "Can't open file '%s' for reading: %s [errno %d]"
                errstr = string.format(errstr, self.taskDataSet[name][1],
                                       errno.strerror(), errno())
                assert(file ~= nil, errstr)
                local function iterator()
                    while true do
                        local input
                        if line:find('\n') == nil then
                            local rv = file:read(65536)
                            if #rv == 0 then
                                file:close()
                                return nil
                            else
                                line = line .. rv
                            end
                        else
                            input, line = line:match('([^\n]*)\n(.*)')
                            input = json.decode(input)
                            return processDataSet(input)
                        end
                    end
                end
                return iterator, nil
            end,
            iterateDataSetWrap = function(self, name)
                local iter_func = self:iterateDataSet(name)
                local last_item = {}
                local category  = 1
                local iterator = function()
                    if last_item[1] == nil then
                        last_item = iter_func()
                        if last_item == nil then
                            return
                        end
                    end
                    return table.remove(last_item)
                end
                return iterator, nil
            end,
            storeDataSet = function(self, name)
                for tuple in self:iterateDataSetWrap(name) do
                    self.taskDataSet[name][2]:replace(tuple)
                end
            end,
            addAggregators = function(self, instance)
                log.info("<worker_context> Adding aggregators")
                for taskName, _ in pairs(self.taskPhases) do
                    instance:add_aggregator(taskName, {
                        default = {
                            name     = nil,
                            command  = message_command.NONE,
                            target   = 0.0,
                            features = {}
                        },
                        merge   = function(old, new)
                            if new ~= nil and
                               (old == nil or new.command > old.command) then
                                return deepcopy(new)
                            end
                            return old
                        end
                    })
                end
                return instance
            end
        }
    })

    if worker == 'worker' then
        for _, task_config in ipairs(input) do
            local name   = task_config['name']
            local input  = task_config['input']
            local fname  = check_file(fio.pathjoin(DATASET_PATH, input))
            local sname  = ('wc_%s_data_set'):format(name)
            local fspace = box.space[sname]
            taskDataSet[name] = {fname, fspace}
            if box.space[sname] == nil then
                local space = box.schema.create_space(sname, {
                    format = {
                        [1] = {name = 'id_type',  type = 'str'},
                        [2] = {name = 'id',       type = 'str'},
                        [3] = {name = 'category', type = 'num'}
                    }
                })
                space:create_index('primary', {
                    type  = 'TREE',
                    parts = {1, 'STR', 2, 'STR'}
                })
                taskDataSet[name][2] = space
                log.info("<worker_context> Begin preloading data for '%s'", name)
                wc:storeDataSet(name)
                log.info("<worker_context> Data stored for '%s'", name)
            end
            log.info("<worker_context> Done loading dataSet for '%s'", name)
        end
    end

    log.info('<worker_context> Initialized:')
    log.info('<worker_context> taskDeploymentConfigs:')
    fun.iter(taskDeploymentConfigs):each(function(name, config)
        log.info('<worker_context> %s -> %s', name, json.encode(config))
    end)
end

local node_common_methods = {
    get_status = function(self)
        return self:get_value().status or node_status.UNKNOWN
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
        log.info('<MASTER> Initializing')
    end,
    work = function(self)
        local value = self:get_value()

        if self:get_superstep() == 1 then
            for name, _ in pairs(wc.taskPhases) do
                log.info('<MASTER> Adding task vertex %s', name)
                self:add_vertex({
                    name     = name,
                    vtype    = vertex_type.TASK,
                    features = value.features,
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
        local taskName = self:get_value().name
        log.info('<task node, %s> Initializing', taskName)
        if self:get_status() == node_status.NEW then
            self:set_status(node_status.WORKING)
        end
        local space_name = ('task_node_%s_data_set'):format(taskName)
        if box.space[space_name] == nil then
            local space = box.schema.create_space(space_name, {
                format = {
                    [1] = {name = 'id',           type = 'num'  },
                    [2] = {name = 'task_name',    type = 'str'  },
                    [3] = {name = 'suffix',       type = 'str'  },
                    [4] = {name = 'target_round', type = 'num'  },
                    [5] = {name = 'target',       type = 'num'  },
                    [6] = {name = 'features',     type = 'array'},
                }
            })
            space:create_index('primary', {
                type  = 'TREE',
                parts = {1, 'NUM'}
            })
            space:create_index('name', {
                type   = 'TREE',
                parts  = {2, 'STR', 3, 'STR', 4, 'NUM'},
                unique = false
            })
        end
        self.dataSetSpace = box.space[space_name]
    end,
    work = function(self)
        local value    = self:get_value()
        local taskName = self:get_value().name
        local phase    = wc:getTaskPhase(taskName)

        if phase == task_phase.SELECTION then

            log.info('<task node, %s> SELECTION phase', taskName)
            for _, task in wc.taskDataSet[taskName][2]:pairs() do
                local ktype, kname, value = task:unpack()
                local name = ('%s:%s'):format(ktype, kname)
                -- log.info('<iterateDataSet, %s> send_message to <%s>', taskName, name)
                self:send_message(name, {
                    sender   = self:get_name(),
                    command  = message_command.FETCH,
                    target   = value,
                    features = NULL
                })
            end
            log.info('<task node, %s> SELECTION phase done', taskName)

            wc:setTaskPhase(taskName, task_phase.TRAINING)
        elseif phase == task_phase.TRAINING then

            log.info('<task node, %s> TRAINING phase', taskName)

            local testVerticesFraction = GDParams['test.vertices.fraction']
            log.info('Test vertices fraction %f', testVerticesFraction)

            local n, recordCounts = self:saveDataSetToLocalSpace(
                taskName, testVerticesFraction
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
            local alpha          = GDParams['gd.loss.averaging.factor']
            local epsilon        = GDParams['gd.loss.convergence.factor']

            local params = self:train(taskName, recordCounts, d, maxIter,
                                      trainBatchSize, testBatchSize, alpha,
                                      epsilon)
            value.features = params
            self:set_value(value)

            local nCalibrationMessages = GDParams['n.calibration.vertices']
            log.info('Number of calibration messages: %d', nCalibrationMessages)

            local calibrationProb = math.min(
                1.0 * nCalibrationMessages / #wc.randomVertexIds, 1.0
            )
            log.info('Probability to take message for calibration: %f',
                     calibrationProb)

            local n = 0
            for idx, randomID in wc:iterateRandomVertexIds() do
                if math.random() < calibrationProb then
                    self:send_message(randomID, {
                        sender   = self:get_name(),
                        command  = message_command.PREDICT_CALIBRATION,
                        target   = 0.0,
                        features = value.features,
                    })
                    n = n + 1
                end
            end
            log.info('sent %d calibration messages. Waiting for response', n)

            wc:setTaskPhase(taskName, task_phase.CALIBRATION)
        elseif phase == task_phase.CALIBRATION then

            log.info('<task node, %s> CALIBRATION phase', taskName)
            local ds = algo.PercentileCounter()
            for _, msg, _ in self:pairs_messages() do
                ds:addValue(msg.target)
            end
            if ds:getN() == 0 then
                log.warn('Master didn\'t receive any messages, so no calibration occured')
                log.warn('Waiting one superstep')
                return
            end
            local cbp = GDParams['calibration.bucket.percents']
            log.info('Calibration bucket percents %f', cbp)
            local parametersAndCalibration = self:calibrate(value.features, ds,
                                                            #value.features, cbp)
            log.info('parametersAndCalibration %d', #parametersAndCalibration)
            local broadcast = {
                sender   = self:get_name(),
                command  = message_command.PREDICT,
                target   = 0.0,
                features = parametersAndCalibration
            }
            log.info('<task node, %s> Set aggregator to broadcast model ' ..
                     'across all vertices to:', taskName)
            log.info('<task node, %s> - command - PREDICT', taskName)
            log.info("<task node, %s> - sender: %s", taskName, broadcast.sender)
            log.info('<task node, %s> - features:', taskName)
            log_features(("task node, %s"):format(taskName), broadcast.features)
            self:set_aggregation(taskName, broadcast)

            wc:setTaskPhase(taskName, task_phase.PREDICTION)
        elseif phase == task_phase.PREDICTION then

            log.info('<task node, %s> PREDICTION phase', taskName)
            log.info("Calibrated data:")
            local n = 1
            for _, msg in self:pairs_messages() do
                log.info('%d> %f', n, msg.target)
                n = n + 1
            end
            if n == 0 then
                log.info('Master didn\'t receive any messaged, so no prediction occured')
                log.info('Waiting one superstep')
                return
            end

            wc:setTaskPhase(taskName, task_phase.DONE)
        elseif phase == task_phase.DONE then

            log.info('<task node, %s> DONE phase', taskName)
            self:vote_halt()

            self:set_status(node_status.INACTIVE)
        else
            assert(false)
        end
    end,
    calibrate = function(self, param, ds, dim, calibrationBucketPercents)
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1;
        log.info('Model dimensionality: %d, calibration bucket percents: %f',
                 dim, calibrationBucketPercents)
        log.info('Number of calibration percentiles: %d', nPercentiles)
        local parametersWithCalibration = fun.iter(param):chain(
            fun.range(1, nPercentiles):map(function(p)
                return ds:getPercentile((p + 1) * calibrationBucketPercents)
            end)
        ):totable()
        return parametersWithCalibration
    end,
    train = function(self, taskName, recordounts, dim, maxIter,
                     trainBatchSize, testBatchSize, alpha, epsilon)
        log.info('<task node, %s> Initializing Gradient descent',    taskName)
        log.info('<task node, %s> - train batch size %d',            taskName, trainBatchSize)
        log.info('<task node, %s> - test batch size %d',             taskName, testBatchSize)
        log.info('<task node, %s> - maximum number of iteration %d', taskName, maxIter)
        log.info('<task node, %s> - loss averaging factor %f',       taskName, alpha)
        log.info('<task node, %s> - loss convergence factor %f',     taskName, epsilon)

        local gd = algo.GradientDescent('hinge', 'l2')
        local param = gd:initialize(dim)
        log.info('<task node, %s> initialized model parameters to:', taskName)
        log_features(('task node, %s'):format(taskName), param)

        local trainAverageLoss = nil
        local testAverageLoss  = nil

        local function get_rtargets(self)
            local acc = {n = 0}
            self.dataSetSpace.index.name:pairs{taskName, 'test'}
                                        :each(function(tuple)
                local target_round = tuple[4]
                if acc[acc.n] ~= target_round then
                    table.insert(acc, target_round)
                    acc.n = acc.n + 1
                end
            end)
            return acc
        end

        local rtargets = get_rtargets(self)

        for nIter = 1, maxIter do
            local trainBatchLoss = 0.0
            local testBatchLoss  = 0.0
            local trainBatchGradient = fun.duplicate(0.0):take(dim):totable()
            for _, rtarget in ipairs(rtargets) do
                do
                    local batchSize = testBatchSize
                    while batchSize > 0 do
                        self.dataSetSpace.index.name:pairs{taskName, 'test', rtarget}
                                                    :take(batchSize)
                                                    :all(function(tuple)
                            local target, features = tuple:unpack(5, 6)
                            -- log.info('- msgid: %d', tuple[1])
                            -- log.info('- target: %f', target)
                            -- log.info('- features')
                            -- log_features(('task node, %s'):format(taskName), features, 500)
                            local lg = gd:lossAndGradient(target, features, param)
                            testBatchLoss = testBatchLoss + lg[1] / testBatchSize
                            -- if fun.iter(lg):all(function(val) return val == 0 end) == true then
                            --     log.info('<task node, %s> lossAndGradient has zeroed result', taskName)
                            -- end
                            batchSize = batchSize - 1
                            -- log.info('"test" message processed, %d left', batchSize)
                            return batchSize > 0
                        end)
                    end
                end
                do
                    local batchSize = trainBatchSize
                    while batchSize > 0 do
                        self.dataSetSpace.index.name:pairs{taskName, 'train', rtarget}
                                                    :take(batchSize)
                                                    :all(function(tuple)
                            local target, features = tuple:unpack(5, 6)
                            -- log.info('- target: %f', target)
                            -- log.info('- features')
                            -- log_features(('task node, %s'):format(taskName), features, 500)
                            local lg = gd:lossAndGradient(target, features, param)
                            trainBatchLoss = trainBatchLoss + lg[1] / trainBatchSize
                            for i = 2, #lg do
                                trainBatchGradient[i - 1] = trainBatchGradient[i - 1] + lg[i]
                            end
                            -- if fun.iter(lg):all(function(val) return val == 0 end) == true then
                            --     log.info('<task node, %s> lossAndGradient has zeroed result', taskName)
                            -- end
                            batchSize = batchSize - 1
                            -- log.info('"train" message processed, %d left', batchSize)
                            return batchSize > 0
                        end)
                    end
                end
            end
            if trainAverageLoss == nil then
                trainAverageLoss = trainBatchLoss
            else
                trainAverageLoss = (1 - alpha) * trainAverageLoss +
                                         alpha * trainBatchLoss
            end

            if testAverageLoss == nil then
                testAverageLoss = testBatchLoss
            else
                local tal = (1 - alpha) * testAverageLoss +
                                  alpha * testBatchLoss
                if math.abs(testAverageLoss - tal) < epsilon then
                    log.info('<task node, %s> GD converged on iteration %d',
                             taskName, nIter)
                    break
                end
                testAverageLoss = tal
            end

            param = gd:update(nIter, param, trainBatchGradient)
            log.info('<task node, %s> GD OUTPUT on iteration %d:', taskName, nIter)
            log.info('<task node, %s> trainBatchLoss - %f, testBatchLoss - %f',
                     taskName, trainBatchLoss, testBatchLoss)
            log.info('<task node, %s> trainAverageLoss - %f, testAverageLoss - %f',
                     taskName, trainAverageLoss, testAverageLoss)
        end

        log.info('<task node, %s> Finished GD, new parameters', taskName)
        log_features(('task node, %s'):format(taskName), param)

        self.dataSetSpace:truncate()
        return param
    end,
    saveDataSetToLocalSpace = function(self, taskName, testVerticesFraction)
        log.info('<task node, %s> Saving data to space %s',
                 taskName, self.dataSetSpace.name)

        local cnt = 0

        for _, msg in self:pairs_messages() do
            local rtarget = math_round(msg.target)
            local suffix = math.random() < testVerticesFraction and SUFFIX_TEST or SUFFIX_TRAIN

            self.dataSetSpace:auto_increment{taskName, suffix, rtarget,
                                             msg.target, msg.features}
            cnt = cnt + 1
        end
        local last = self.dataSetSpace.index.name:select(taskName, {
            limit = 1
        })[1]
        while true do
            if last == nil or last[2] ~= taskName then
                break
            end
            local suffix, target = last:unpack(3, 4)
            local cnt = self.dataSetSpace.index.name:count{taskName, suffix, target}
            log.info('<task node, %s> Written data set: %s_%d_%s -> %d',
                     taskName, taskName, target, suffix, cnt)
            last = self.dataSetSpace.index.name:select(
                {taskName, suffix, target}, {limit = 1, iterator = 'GT'}
            )[1]
        end
        return cnt
    end,
    removeDataSetLocalSpace = function(self, taskName, suffix, target)
        log.info('<task node, %s> Removing all records for %s_%d_%s',
                 taskName, taskName, suffix, target)
        local to_remove = self.dataSetSpace.index.name
                                           :pairs{taskName, suffix, target}
                                           :map(function(tuple)
            return tuple[1]
        end):totable()
        fun.iter(to_remove):each(function(id)
            self.dataSetSpace:delete(id)
        end)
        log.info('<task node, %s> Removed %d records', #to_remove)
    end,
}

local node_data_methods = {
    __init = function(self)
        local status = self:get_status()
        local negativeVerticesFraction = GDParams['negative.vertices.fraction']
        if status == node_status.NEW then
            local hash = digest.crc32(self:get_name()) % 1000
            if hash <= 1000 * negativeVerticesFraction then
                wc:addRandomVertex(self:get_name())
            end
            self:set_status(node_status.WORKING)
        elseif status == node_status.UNKNOWN then
            self:set_status(node_status.INACTIVE)
        end
    end,
    work = function(self)
        for _, msg in self:pairs_messages() do
            -- return feature vector to master
            if msg.command == message_command.FETCH then
                -- log.info("<data node, '%s'->'%s'> processing command FETCH",
                --          self:get_name(), msg.sender)
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = msg.target,
                    features = self:get_value().features
                })
            -- compute raw prediction and return to master
            elseif msg.command == message_command.PREDICT_CALIBRATION then
                -- log.info("<data node, '%s'->'%s'> processing command PREDICT_CALIBRATION",
                --          self:get_name(), msg.sender)
                local prediction = self:predictRaw(msg.features)
                -- log.info('<data node, "%s"> prediction is %f', self:get_name(), prediction)
                self:send_message(msg.sender, {
                    sender   = self:get_name(),
                    command  = message_command.NONE,
                    target   = prediction,
                    features = NULL
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
            if msg ~= nil and msg.command == message_command.PREDICT then
                -- log.info('<data node, %s> processing command PREDICT from aggregator', self:get_name())
                isPredictionPhase = true
                local calibratedPrediction = self:predictCalibrated(
                    msg.features, wc.calibrationBucketPercents
                )
                -- log.info('<data node, %s> calibratedPrediction %f', self:get_name(), calibratedPrediction)

                -- return to master for report, maybe
                if math.random() < wc.predictionReportSamplingProb then
                    self:send_message(msg.sender, {
                        sender   = self:get_name(),
                        command  = message_command.NONE,
                        target   = calibratedPrediction,
                        features = NULL
                    })
                end

                -- write (audience, score) pair if threshold exceeded
                local dcName = ('%s:%s'):format(taskName, self.idType)
                local dc = wc.taskDeploymentConfigs[dcName]
                local maxPredictedCalibratedValue = GDParams['max.predicted.calibrated.value']
                if (dc ~= nil and
                    calibratedPrediction > maxPredictedCalibratedValue *
                                           (1 - dc.threshold)) then
                    table.insert(predictions, {dc.targeting, calibratedPrediction})
                end
            end
        end

        if isPredictionPhase == true then
            if #predictions > 0 then
                log.info('<data node, %s> Prediction phase done', self:get_name())
                log.info('<data node, %s> %s', self:get_name(), json.encode(predictions))
            end
            -- Update value
            local value = self:get_value()
            value.features = predictions
            self:set_value(value)
            -- Make it inactive
            self:set_status(node_status.INACTIVE)
            self:vote_halt()
        end
    end,
    predictRaw = function(self, parameters)
        local features = self:get_value().features
        local prediction = fun.iter(features):zip(parameters)
                              :map(function(feature, parameter)
            return feature * parameter
        end):sum()
        return prediction
    end,
    predictCalibrated = function(self, param, calibrationBucketPercents)
        local features = self:get_value().features
        local dim = #features
        local nPercentiles = math.floor(100 / calibrationBucketPercents) - 1
        local percentileStep = GDParams['max.predicted.calibrated.value'] / 100
              percentileStep = math.floor(percentileStep * calibrationBucketPercents)
        local prediction = self:predictRaw(param)

        local calibratedPrediction = nil
        -- log.info('param_len %d', #param)
        -- log.info('featu_len %d', dim)
        for i = 1, nPercentiles do
            if prediction < param[dim + i] then
                calibratedPrediction = math.random(0, percentileStep) + i * percentileStep
                break
            end
        end
        if calibratedPrediction == nil then
            calibratedPrediction = math.random(0, percentileStep)
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
        for k, v in pairs(vertex_mt.__index) do
            node_master_mt.__index[k] = v
            node_task_mt.__index[k] = v
            node_data_mt.__index[k] = v
        end
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

    if vtype == vertex_type.MASTER then
        setmetatable(vertex, node_master_mt)
    elseif vtype == vertex_type.TASK then
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

local function generate_worker_uri(cnt)
    return fun.range(cnt or 4):map(function(k)
        return 'localhost:' .. tostring(3301 + k)
    end):totable()
end

local common_cfg = {
    master         = 'sh7.tarantool.org:3301',
    workers        = generate_worker_uri(8),
    compute        = computeGradientDescent,
    combiner       = nil,
    master_preload = avro_loaders.master,
    worker_preload = avro_loaders.worker_additional,
    preload_args   = {
        path          = DATASET_PATH,
        feature_count = 300,
        vertex_count  = 17600000,
    },
    squash_only    = false,
    pool_size      = 250,
    delayed_push   = false,
    obtain_name    = utils.obtain_name
}

if worker == 'worker' then
    worker = pworker.new('test', common_cfg)
    wc:addAggregators(worker)
else
    xpcall_tb(function()
        local master = pmaster.new('test', common_cfg)
        wc:addAggregators(master)
        master:wait_up()
        if arg[1] == 'load' then
            -- master:preload()
            master:preload_on_workers()
            master.mpool:send_wait('snapshot')
        end
        master.mpool:by_id('MASTER:'):put('vertex.store', {
            key      = {vid = 'MASTER', category = 0},
            features = fun.duplicate(0.0):take(#wc.featureList):totable(),
            vtype    = constants.vertex_type.MASTER,
            status   = constants.node_status.NEW
        })
        master.mpool:flush()
        master:start()
    end)
    os.exit(0)
end
