--- Error raising, tracebacks and small type tests shared across pregel.
--
-- The `error` and `syserror` exported here shadow the global error() on
-- purpose: they take a printf-style message, which is what almost every call
-- site in this library wants, and they get the level arithmetic right so the
-- position in the raised message names the caller rather than this file.
--
-- @module pregel.utils

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

--- Raise a formatted error. Usage: error([level, ] message [, format_args...])
--
-- With a single argument the message is raised verbatim -- see safe_format(),
-- a stray '%' in a re-raised message must not turn into a formatting failure.
-- A leading number is the level in the sense of the standard error(): 1 (the
-- default) blames the caller of this function, 2 its caller, and 0 prefixes no
-- position at all, which is what makes the message comparable for equality.
--
-- @param ... optional level, then the message and its format arguments
-- @raise always
-- @function error
local function error(...)
    local level, args, n = split_level(...)
    basic_error(safe_format(n, args), level)
end

--- Raise a formatted error about a failed syscall.
-- Usage: syserror([level, ] message [, format_args...])
--
-- Wraps the message as '[errno N] <message>: <strerror>'. errno is read at the
-- moment of the call, so this has to be the first thing done after the failing
-- operation -- anything in between can overwrite it.
--
-- @param ... optional level, then the message and its format arguments
-- @raise always
-- @function syserror
local function syserror(...)
    local level, args, n = split_level(...)
    basic_error(fmtstring('[errno %d] %s: %s', errno(), safe_format(n, args),
                          errno.strerror()), level)
end

--- Walk the call stack upwards and describe every frame above the caller.
--
-- The walk starts `ldepth` frames above traceback()'s own caller, so the
-- default of 1 skips that caller too: the frames of interest are the ones that
-- led to it. The list runs outwards, innermost frame first, and stops at the
-- bottom of the stack.
--
-- @param ldepth number of extra frames to skip (default 1)
-- @return array of {line = number, file = string, what = string, name = string
--         or nil}
-- @function traceback
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
--
-- The handler runs on the still-live stack, which is the only place the frames
-- below the error exist -- a pcall() plus a traceback afterwards sees none of
-- them. Arguments are forwarded through a closure that preserves their count,
-- so trailing nils reach `func`.
--
-- @param func callable to run
-- @param ... arguments for it
-- @return false and the error, or true and everything `func` returned
-- @function xpcall_tb
local function xpcall_tb(func, ...)
    return xpcall(lazy_func(func, ...), xpcall_tb_cb)
end

--- Run func(...) and return how much CPU time it used, in seconds.
--
-- os.clock(), not wall clock: a call that sleeps, yields to another fiber or
-- waits on the network is charged almost nothing. Return values of `func` are
-- discarded.
--
-- @param func callable to run
-- @param ... arguments for it
-- @return number of seconds of CPU time
-- @function timeit
local function timeit(func, ...)
    local time = os.clock()
    func(...)
    return os.clock() - time
end

--- True for a function, and for a table whose metatable has a __call function.
--
-- Used to vet every user-supplied callback (combiners, aggregator reduce/merge,
-- loaders), which is why the callable object case matters: loaders are tables.
--
-- @param arg any value
-- @return boolean
-- @function is_callable
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
