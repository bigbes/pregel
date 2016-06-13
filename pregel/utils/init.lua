local log = require('log')
local errno = require('errno')

local strict = require('pregel.utils.strict')

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

local function syserror(...)
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

local function traceback(ldepth)
    local tb = {}
    local depth = 2 + (ldepth or 1)
    local level = depth
    while true do
        local info = debug.getinfo(level)
        if info == nil then
            break
        elseif type(info) ~= 'table' then
            log.error('unsupported `info` type: %s', type(info))
            break
        end
        local line = info.currentline or 0
        local file = info.short_src or info.src or 'eval'
        local what = info.what or 'undef'
        local name = info.name
        table.insert(tb, {
            line = line,
            file = file,
            what = what,
            name = name
        })
        level = level + 1
    end
    return tb
end

local function log_traceback(ldepth)
    ldepth = ldepth or 2
    for _, f in pairs(traceback()) do
        local name = f.name and fmtstring(" function '%s'", f.name) or ''
        log.info("[%-4s]%s at <%s:%d>", f.what, name, f.file, f.line)
    end
end

local function lazy_func(func, ...)
    local arg = {...}
    return function()
        return func(unpack(arg))
    end
end

local function xpcall_tb_cb(err)
    err = err or '<none>'
    log.error("Error catched: %s", err)
    for _, f in pairs(traceback()) do
        local name = f.name and fmtstring(" function '%s'", f.name) or ''
        log.error("[%-4s]%s at <%s:%d>", f.what, name, f.file, f.line)
    end
    return err
end

local function xpcall_tb(func, ...)
    return xpcall(lazy_func(func, ...), xpcall_tb_cb)
end

local function timeit(func, ...)
    local time = os.clock()
    func(...)
    return os.clock() - time
end

local function is_callable(arg)
    if arg ~= nil then
        local mt = (type(arg) == 'table' and getmetatable(arg) or nil)
        if type(arg) == 'function' or mt ~= nil and type(mt.__call) == 'function' then
            return true
        end
    end
    return false
--[[ ALTERNATIIVE FORM
    if arg == nil then
        return false
    end
    if type(arg) == 'function' then
        return true
    end
    local mt = (type(arg) == 'table' and getmetatable(arg) or nil)
    if mt ~= nil and type(mt.__call) == 'function' then
        return true
    end
    return false
]]--
end

local function is_main()
    return debug.getinfo(2).what == "main" and pcall(debug.getlocal, 5, 1) == false
end

local function random(x, y)
    while true do
        if x == nil then
            local rv = math.random()
            if rv >= 0 and rv <= 1 then
                return rv
            end
        elseif y == nil then
            local rv = math.random(x)
            if rv >= 1 and rv <= x then
                return rv
            end
        else
            local rv = math.random(x, y)
            if rv >= x and rv <= y then
                return rv
            end
        end
    end
end

local function execute_authorized_mr(user, fun, ...)
    local puser = box.session.user()
    box.session.su(user)
    local args = {fun(...)}
    box.session.su(puser)
    return unpack(args)
end

return strict.strictify({
    error                 = error,
    random                = random,
    syserror              = syserror,
    traceback             = traceback,
    log_traceback         = log_traceback,
    lazy_func             = lazy_func,
    xpcall_tb             = xpcall_tb,
    timeit                = timeit,
    is_callable           = is_callable,
    is_main               = is_main,
    execute_authorized_mr = execute_authorized_mr
})
