--- Table constructors.
--
-- @module pregel.utils.collections

local strict = require('pregel.utils.strict')

--- A table that fills a missing key in on first read.
--
-- `factory` is either a value stored in every missing key, or a function
-- called with the key. Reading a key always materialises it, so `pairs()` over
-- a defaultdict only sees keys that were read or written -- which is why the
-- queue's counters are read with rawget() where a read must not create one.
--
-- A non-function `factory` is stored by reference and not copied, so a table
-- default ends up shared by every key that materialises from it. Pass a
-- function when each key needs its own.
--
-- @param factory value stored in a missing key, or function(key) returning one
-- @return table
-- @function defaultdict
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
