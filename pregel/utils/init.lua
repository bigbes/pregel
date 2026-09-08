local log   = require('log')
local errno = require('errno')

local strict = require('pregel.utils.strict')

local basic_error = error
local fmtstring   = string.format

--- Render a message that was passed as (format_string, ...).
--
-- string.format() fails on a stray '%' in the message and on a missing
-- argument, and both happen with messages that were never meant as format
-- strings -- re-raising a caught error through error(err) is the common case.
-- Losing the message and reporting the formatting failure instead is what this
-- avoids: a lone argument is never formatted, and a failed format keeps the
-- original message and appends the reason.
local function safe_format(n, args)
    if n <= 1 then
        return tostring(args[1])
    end
    local stat, text = pcall(fmtstring, unpack(args, 1, n))
    if stat == true then
        return text
    end
    return fmtstring('%s [format failed: %s]', tostring(args[1]), tostring(text))
end

--- Split an optional leading numeric level off an argument list.
--
-- Returns the level, the remaining arguments and their count.
local function split_level(...)
    local n = select('#', ...)
    local args = {...}
    local level = 1
    if type(args[1]) == 'number' then
        level = table.remove(args, 1)
        n = n - 1
    end
    -- Level 1 means "blame the caller of error()". This function is one frame
    -- deeper than the plain error() it stands in for, so every non-zero level
    -- shifts by one.
    if level ~= 0 then
        level = level + 1
    end
    return level, args, n
end

--- Usage: error([level, ] message [, format_args...])
local function error(...)
    local level, args, n = split_level(...)
    basic_error(safe_format(n, args), level)
end

--- Usage: syserror([level, ] message [, format_args...])
--
-- Appends the current errno and its description to the message.
local function syserror(...)
    local level, args, n = split_level(...)
    basic_error(fmtstring('[errno %d] %s: %s', errno(), safe_format(n, args),
                          errno.strerror()), level)
end

local function traceback(ldepth)
    local tb = {}
    local level = 2 + (ldepth or 1)
    while true do
        local info = debug.getinfo(level)
        if info == nil then
            break
        elseif type(info) ~= 'table' then
            log.error('unsupported `info` type: %s', type(info))
            break
        end
        table.insert(tb, {
            line = info.currentline or 0,
            file = info.short_src or info.src or 'eval',
            what = info.what or 'undef',
            name = info.name
        })
        level = level + 1
    end
    return tb
end

local function lazy_func(func, ...)
    local args = {...}
    local n = select('#', ...)
    return function()
        return func(unpack(args, 1, n))
    end
end

local function xpcall_tb_cb(err)
    err = err or '<none>'
    log.error('Error caught: %s', tostring(err))
    for _, f in ipairs(traceback()) do
        local name = f.name and fmtstring(" function '%s'", f.name) or ''
        log.error('[%-4s]%s at <%s:%d>', f.what, name, f.file, f.line)
    end
    return err
end

--- xpcall() that logs a traceback of the failing frame before unwinding.
local function xpcall_tb(func, ...)
    return xpcall(lazy_func(func, ...), xpcall_tb_cb)
end

--- Run func(...) and return how long it took, in seconds.
local function timeit(func, ...)
    local time = os.clock()
    func(...)
    return os.clock() - time
end

local function is_callable(arg)
    if type(arg) == 'function' then
        return true
    end
    local mt = (type(arg) == 'table' and getmetatable(arg) or nil)
    return mt ~= nil and type(mt.__call) == 'function'
end

return strict.strictify({
    error       = error,
    syserror    = syserror,
    traceback   = traceback,
    xpcall_tb   = xpcall_tb,
    timeit      = timeit,
    is_callable = is_callable,
})
