local fio = require('fio')
local json = require('json')
local fiber = require('fiber')
local errno = require('errno')

local elog_global = nil
local elog_list = {}

local error = require('pregel.utils').error
local syserror = require('pregel.utils').syserror
local traceback = require('pregel.utils').traceback

local strict = require('pregel.utils.strict')

local checkt_xc = require('pregel.utils.checktype').checkt_xc

local iter = require('fun').iter

local fmtstring = string.format

local CRITICAL = 60
local ERROR    = 50
local WARNING  = 40
local INFO     = 30
local VERBOSE  = 20
local DEBUG    = 10

local lvl_char = {
    [CRITICAL] = '!',
    [ERROR]    = 'E',
    [WARNING]  = 'W',
    [INFO]     = 'I',
    [VERBOSE]  = 'V',
    [DEBUG]    = 'D'
}

local function elog_close(self)
    if self.fd == nil then return end
    self.fd:fsync()
    self.fd:close()
    self.fd = nil
end

local function convert_level(level)
    level = level or INFO
    if type(level) == 'string' then
        if level == 'CRITICAL' or level == 'critical' then
            level = CRITICAL
        elseif level == 'ERROR' or level == 'error' then
            level = ERROR
        elseif level == 'WARNING' or level == 'warning' then
            level = WARNING
        elseif level == 'INFO' or level == 'info' then
            level = INFO
        elseif level == 'VERBOSE' or level == 'verbose' then
            level = VERBOSE
        elseif level == 'DEBUG' or level == 'debug' then
            level = DEBUG
        else
            error("Unknown level name: '%s'", level)
        end
    end
    if type(level) == 'number' ~= true then
        error("Unknown level type: %s", json.encode(level))
    end
    return level
end

local function elog_traceback(self, level)
    level = convert_level(level)
    if self.level > level then return end
    local tb = traceback(1)
    for _, f in ipairs(traceback()) do
        local name = f.name and fmtstring(" function '%s'", f.name) or ''
        self:logl(level, "[%-4s]%s at <%s:%d>", f.what, name, f.file, f.line)
    end
end

local function elog_log(self, level, fmt, ...)
    level = convert_level(level)
    if self.level > level then return end
    local args = {...}
    for k, v in pairs(args) do
        if type(v) == 'table' then
            args[k] = json.encode(v)
        end
    end
    local fiber_time = fiber.time()
    local os_time = os.time()
    local ms = math.floor((fiber_time - os_time) * 1000)
    local ident = lvl_char[(math.floor(level / 10) + 1) * 10]
    fmt = string.format("%s.%03d %s> " .. fmt .. "\n",
                        os.date("%Y-%m-%d %H:%M:%S"),
                        ms, ident, unpack(args))
    self.fd:write(fmt)
    self.fd:fsync()
end

local function elog_closure_log(level)
    return function (self, fmt, ...)
        self:logl(level, fmt, ...)
    end
end

local elog_mt = {
    close     = elog_close,
    tb        = elog_traceback,
    traceback = elog_traceback,
    logl      = elog_log,
    debug     = elog_closure_log(DEBUG),
    verbose   = elog_closure_log(VERBOSE),
    info      = elog_closure_log(INFO),
    warning   = elog_closure_log(WARNING),
    error     = elog_closure_log(ERROR),
}

local function elog_init(cfg)
    checkt_xc(cfg.path, 'string', 'cfg.path', 2)
    checkt_xc(cfg.level, {'number', 'nil'}, 'cfg.level', 2)
    cfg.level = cfg.level or 30
    local instance = {
        path  = cfg.path,
        level = cfg.level,
        fd    = fio.open(cfg.path, {'O_CREAT', 'O_WRONLY', 'O_APPEND'}, tonumber('0644', 8))
    }
    if instance.fd == nil then
        syserror(2, "Failed to open logger")
    end
    return setmetatable(instance, {
        __index = elog_mt,
        __gc    = elog_close
    })
end

return setmetatable({
    CRITICAL = CRITICAL,
    ERROR = ERROR,
    WARNING = WARNING,
    INFO = INFO,
    VERBOSE = VERBOSE,
    DEBUG = DEBUG,
}, {
    __call = function(self, name, opts)
        if type(name) == 'nil' or type(name) == 'table' then
            elog_global = elog_global or elog_init(name or {})
            return elog_global
        end
        checkt_xc(name, 'string', 'name')
        elog_list['name'] = elog_list['name'] or elog_init(opts or {})
        return elog_list['name']
    end
})
