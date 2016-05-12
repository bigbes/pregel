local strict = require('pregel.utils.strict')

local function defaultdict_index(factory)
    return function(self, key)
        if type(factory) == 'function' then
            self[key] = factory(key)
        else
            self[key] = factory
        end
        return self[key]
    end
end

local function defaultdict(factory)
    return setmetatable({}, {
        __index = defaultdict_index(factory)
    })
end

local function readonly_table(table)
    return setmetatable({}, {
        __index = table,
        __newindex = function(table, key, value)
            error("Attempt to modify read-only table")
        end,
        __metatable = false
    });
end

return strict.strictify({
    defaultdict = defaultdict,
    rotable = readonly_table
})
