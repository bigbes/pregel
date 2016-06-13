local constants = require('constants')

local vertex_type = constants.vertex_type
local dataSetKeys = constants.dataSetKeys

local MASTER_VERTEX_TYPE = constants.MASTER_VERTEX_TYPE
local TASK_VERTEX_TYPE   = constants.TASK_VERTEX_TYPE

local function obtain_type(name)
    return name:match('(%a+):(%w*)')
end

local function obtain_name(value)
    if value.vtype == vertex_type.MASTER then
        return ('%s:%s'):format(MASTER_VERTEX_TYPE, nil)
    elseif value.vtype == vertex_type.TASK then
        return ('%s:%s'):format(TASK_VERTEX_TYPE, value.name)
    end
    for _, name in ipairs(dataSetKeys) do
        local key_value = value.key[name]
        if key_value ~= nil and
           type(key_value) == 'string' and
           #key_value > 0 then
            if key_value == 'vid' and name == MASTER_VERTEX_TYPE then
                return 'MASTER:'
            end
            return ('%s:%s'):format(name, key_value)
        end
    end
    assert(false)
end

return {
    obtain_type = obtain_type,
    obtain_name = obtain_name,
}
