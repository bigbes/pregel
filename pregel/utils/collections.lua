local strict = require('pregel.utils.strict')

--- A table that fills a missing key in on first read.
--
-- `factory` is either a value copied into every missing key, or a function
-- called with the key. Reading a key always materialises it, so `pairs()` over
-- a defaultdict only sees keys that were read or written.
local function defaultdict(factory)
    local index
    if type(factory) == 'function' then
        index = function(self, key)
            local value = factory(key)
            rawset(self, key, value)
            return value
        end
    else
        index = function(self, key)
            rawset(self, key, factory)
            return factory
        end
    end
    return setmetatable({}, { __index = index })
end

return strict.strictify({
    defaultdict = defaultdict,
})
