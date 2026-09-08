local t = require('luatest')

local g = t.group('smoke')

g.test_environment = function()
    local utils = require('pregel.utils')
    t.assert_equals(utils.is_callable(print), true)
    t.assert_ge(tonumber(_TARANTOOL:match('^(%d+)')), 3)
end
