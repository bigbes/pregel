local log = require('log')
local errno = require('errno')

local checkt_xc = require('pregel.utils.checktype').checkt_xc

local basic_error = error
local fmtstring   = string.format

-- Usage: error([level, ] format_string [, ...])
local error = function(...)
    local args = {...}
    local level = 1
    if type(args[1]) == 'number' then
        level = args[1]
        table.remove(args, 1)
    end
    local stat, err_text = pcall(fmtstring, unpack(args))
    if stat == false then
        error(err_text, 2)
    end
    basic_error(err_text, level)
end

local syserror = function(...)
    local args = {...}
    local level = 1
    if type(args[1]) == 'number' then
        level = args[1]
        table.remove(args, 1)
    end
    args[1] = "[errno %d] " .. args[1] .. ": %s"
    table.insert(args, 2, errno())
    table.insert(args, errno.strerror())
    local err_text = fmtstring(unpack(args))
    basic_error(err_text, level)
end

local traceback = function (ldepth)
    local tb = {}
    local depth = 2 + (ldepth or 1)
    local level = depth
    while true do
        local info = debug.getinfo(level)
        if info == nil then break end
        local line, file, what, name = nil, nil, nil, nil
        if type(info) == 'table' then
            line = info.currentline or 0
            file = info.short_src or info.src or 'eval'
            what = info.what or 'undef'
            name = info.name
        end
        tb[level - depth + 1] = {
            line = line,
            file = file,
            what = what,
            name = name
        }
        level = level + 1
    end
    return tb
end

local lazy_func = function(func, ...)
    checkt_xc(func, 'function', 'function', 2)
    local arg = {...}
    return function()
        return func(unpack(arg))
    end
end

local function xpcall_tb_cb(err)
    err = err or '<none>'
    log.error("Error catched: %s", err)
    for f in ipairs(traceback(o)) do
        local name = f.name and fmtstring(" function '%s'", f.name) or ''
        log.error("[%-4s]%s at <%s:%d>", f.what, name, f.file, f.line)
    end
end

local xpcall_tb = function(func, ...)
    return xpcall(lazy_func(func, ...), xpcall_tb_cb)
end

return {
    error = error,
    syserror = syserror,
    traceback = traceback,
    lazy_func = lazy_func,
    xpcall_tb = xpcall_tb
}

