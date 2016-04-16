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

return {
    defaultdict = defaultdict,
}
