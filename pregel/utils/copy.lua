--- Value copying.
--
-- @module pregel.utils.copy

local strict = require('pregel.utils.strict')

--- Recursively copy a value.
--
-- Non-table values are returned as they are. Metatables are not copied and
-- cycles are not detected: the values this is used for (aggregator defaults)
-- are plain trees, and a cyclic one recurses until the stack runs out.
--
-- @param orig any value
-- @return a copy sharing no table with `orig`
-- @function deep
local function deep(orig)
    if type(orig) ~= 'table' then
        return orig
    end
    local copy = {}
    for key, value in pairs(orig) do
        copy[key] = deep(value)
    end
    return copy
end

return strict.strictify({
    deep = deep,
})
