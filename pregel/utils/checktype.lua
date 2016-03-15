local fun = require('fun')
local iter, lor, reduce = fun.iter, fun.operator.lor, fun.reduce

local fmtstring = string.format

local function checkt_xc(var, types, nvar, lvl)
    if type(nvar) == nil or type(nvar) == 'number' then
        lvl = nvar
        nvar = types
        var = nil
    end
    lvl = lvl or 0
    types = type(types) == 'string' and {types} or types
    if reduce(function(acc, x) return (type(var) == x) or acc end, false, types) then
        return true
    end
    local errstr = fmtstring("type error: %s must be one of {%s}, but not %s",
                             nvar, table.concat(types, ", "), type(var))
    error(errstr, 2 + lvl)
end

local function checkt(var, types)
    types = type(types) == 'string' and {types} or types
    return reduce(function(acc, x) return (type(var) == x) or acc end, false)
end

local function tbl_level(element)
    if type(element) == 'table' then
        return iter(element):map(tbl_level):reduce(math.max) + 1
    end
    return 1
end

local function checkt_table_xc(tbl, types, nvar)
    if type(nvar) == nil then
        nvar = types
        var = nil
    end
    types = type(types) == 'string' and {types} or types
    checkt_xc(tbl, 'table', nvar, 1)
    iter(tbl):each(
        function(x) checkt_xc(x, types, fmtstring('"%s" key', nvar), 3) end
    )
    return true
end

local function checkt_table(tbl, types)
    types = type(types) == 'string' and {types} or types
    return checkt(tbl, 'table') and iter(element):reduce(
        function(acc, x) return (acc or checkt(x, types)) end
    )
end

return {
    checkt_xc = checkt_xc,
    checkt = checkt,
    checkt_table_xc = checkt_table_xc,
    checkt_table = checkt_table
}
