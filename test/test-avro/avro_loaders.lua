local fio = require('fio')
local log = require('log')
local fiber = require('fiber')
local clock = require('clock')

local pavro = require('pregel.avro')
local ploader = require('pregel.loader')


--[[--
-- Avro schema (in JSON representation) is:
-- {
--     'type': 'record',
--     'name': 'KeyValuePair',
--     'namespace': 'org.apache.avro.mapreduce',
--     'fields': [{
--         'name': 'key',
--         'type': {
--             'type': 'record',
--             'name': 'User',
--             'namespace': 'ru.mail.avro',
--             'fields': [{
--                 'name': 'vid',
--                 'type': {'type': 'string'}
--             }, {
--                 'name': 'okid',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'email',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'vkid',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'string'}
--                 ]
--             }, {
--                 'name': 'category',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'int'}
--                 ]
--             }, {
--                 'name': 'start',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'long'}
--                 ]
--             }, {
--                 'name': 'end',
--                 'type': [
--                     {'type': 'null'},
--                     {'type': 'long'}
--                 ]
--             }],
--         }
--     }, {
--         'name': 'value',
--         'type': {
--             'type': 'record',
--             'name': 'SparseFeatureVector',
--             'namespace': 'ru.mail.avro',
--             'fields': [{
--                 'name': 'features',
--                 'type': {
--                     'type': 'array',
--                     'items': {
--                         'type': 'record',
--                         'name': 'Feature',
--                         'fields': [{
--                             'name': 'feature_id',
--                             'type': {'type': 'string'}
--                         }, {
--                             'name': 'value',
--                             'type': [
--                                 {'type': 'double'},
--                                 {'type': 'null'}
--                             ]
--                         }, {
--                             'name': 'timestamps',
--                             'type': [{
--                                 'type': 'array',
--                                 'items': {'type': 'int'}
--                             }, {
--                                 'type': 'null'
--                             }]
--                         }]
--                     }
--                 }
--             }],
--         }
--     }]
-- }
--]]--

local function process_avro_file(self, filename)
    log.info('processing %s', filename)
    local avro_file = pavro.open(filename)
    local count = 0
    local begin_time = clock.time()
    while true do
        local line = avro_file:read_raw()
        if line == nil then break end
        assert(line:type() == pavro.RECORD and
                line:schema_name() == 'KeyValuePair')
        local key_object = {}
        local fea_object = {}
        -- parse key
        do
            local key = line:get('key')
            assert(key ~= nil and
                    key:type() == pavro.RECORD and
                    key:schema_name() == 'User')
            for _, v in ipairs{'okid', 'email', 'vkid'} do
                local obj = key:get(v)
                assert(obj:type() == pavro.UNION)
                local obj_value = obj:get():get()
                key_object[v] = obj_value
            end
            -- set category
            local category = key:get('category')
            assert(category ~= nil and
                    category:type() == pavro.UNION)
            local category_value = category:get()
            -- local category_value = category:get('int')
            key_object.category = category_value:get()
            -- set vid
            local vid = key:get('vid')
            assert(vid ~= nil and
                    vid:type() == pavro.STRING)
            local vid_value = vid:get()
            key_object.vid = vid_value
        end
        -- parse value
        do
            local val = line:get('value')
            assert(val:type() == pavro.RECORD and
                    val:schema_name() == 'SparseFeatureVector')
            local features = val:get('features')
            assert(features ~= nil and
                    features:type() == pavro.ARRAY)
            for index, feature in features:iterate() do
                assert(feature ~= nil and
                        feature:type() == pavro.RECORD and
                        feature:schema_name() == 'Feature')
                local fid  = feature:get('feature_id'):get()
                fid = tonumber(fid:match('SVD_(%d+)')) + 1
                local fval = feature:get('value'):get():get()
                local tst  = feature:get('timestamp')
                assert(tst == nil, 'timestamp is not nil')
                fea_object[fid] = {fval, tst}
            end
        end
        self:store_vertex{key = key_object, features = fea_object}
        line:release()
        count = count + 1
    end
    log.info('done processing %d values in %.3f seconds',
                count, clock.time() - begin_time)
    avro_file:close()
    fiber.yield()
end

local function master_avro_loader(master, path)
    local function loader(self)
        local avro_path  = fio.pathjoin(path, '*.avro')
        local avro_files = fio.glob(avro_path);
        table.sort(avro_files)
        log.info('%d found files found in path %s', #avro_files, avro_path)
        for idx, filename in ipairs(avro_files) do
            process_avro_file(self, filename)
        end
    end
    return ploader.new(master, loader)
end

local function worker_avro_loader(worker, path)
    local function loader(self, current_idx, worker_count)
        local avro_path  = fio.pathjoin(path, '*.avro')
        local avro_files = fio.glob(avro_path);
        table.sort(avro_files)
        log.info('%d found files found in path %s', #avro_files, avro_path)
        for idx, filename in ipairs(avro_files) do
            local avrofile_no = tonumber(filename:match('part%-m%-(%d+).avro'))
            if avrofile_no % worker_count == current_idx - 1 then
                process_avro_file(self, filename)
            end
        end
    end
    return ploader.new(worker, loader)
end

return {
    master = master_avro_loader,
    worker = worker_avro_loader,
}
