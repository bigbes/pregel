local log = require('log')

local getinfo = debug.getinfo

--[[--
  Usage:
    require('strict').strict ()        -- <--- please note the (), that mean call
    require('strict').strict ('fatal') -- fatal mode, throw an error
    require('strict').strict ('warn')  -- warning, instead an error
    require('strict').strictify_on  (table)
    require('strict').strictify_off (table)
--]]--

local strict_mt

strict_mt = {
    __fatal = true,
    __declared = {},
    __index    = function (t, n)
        local have = rawget(t, n)
        if have ~= nil then
            return have
        end
        local have = rawget(_G, n)
        if have ~= nil then
            return have
        end
        if not strict_mt.__declared[n] then
            local d = getinfo(2)
            if d and d.what ~= 'C' then
                local msg = "variable '" .. n .. "' is not declared"
                if strict_mt.__fatal then
                    error(msg, 2)
                else
                    log.error(d.short_src .. ':' .. d.currentline .. ": " .. msg)
                end
            end
        end
        return nil
    end,
    __newindex = function (t, n, v)
        local have = rawget(_G, n)
        if have ~= nil or type(v) == 'function' then
            rawset(_G, n, v)
            return
        end
        if not strict_mt.__declared[n] then
            local d = getinfo(2)
            if d and d.what ~= 'C' then
                local msg = "variable '" .. n .. "' is not declared for set"
                if strict_mt.__fatal then
                    error(msg, 2)
                else
                    log.error(d.short_src .. ':' .. d.currentline .. ": " .. msg)
                end
            end
        end
        rawset(_G, n, v)
        return
    end
}

local function strict(_mode)
    local function global (...)
        for _, v in ipairs{...} do
            strict_mt.__declared[v] = true
        end
    end
    local env = {
        global = global;
    }
    env._G = env
    strict_mt.__fatal = true
    if _mode and _mode == 'warn' then
        strict_mt.__fatal = false
    end
    setmetatable(env, strict_mt)
    setfenv(2, env)
end

local function what()
    local d = getinfo(3, "S")
    return d and d.what or "C"
end

local function getaddr(table)
    return tostring(table):match('table: 0x(%x+)')
end

local strictify_mt
strictify_mt = {
    __declared = {},
    __newindex = function (t, n, v)
        local declared = strictify_mt.__declared[getaddr(t)]
        if declared == nil then
            declared = {}
            strictify_mt.__declared[getaddr(t)] = declared
        end
        print('here')
        if not declared[n] then
            print('here inside')
            local w = what()
            if w ~= "main" and w ~= "C" then
                error("assign to undeclared variable '" .. n .. "'", 2)
            end
            declared[n] = true
        end
        rawset(t, n, v)
    end,
    __index = function (t, n)
        local declared = strictify_mt.__declared[getaddr(t)]
        if declared == nil then
            declared = {}
            strictify_mt.__declared[getaddr(t)] = declared
        end
        if not declared[n] and what() ~= "C" then
            error("variable '" .. n .. "' is not declared", 2)
        end
        return rawget(t, n)
    end
}

local function strictify(table)
    local m = getmetatable(table)

    if m == strictify_mt then
    elseif m == nil then
        setmetatable(table, strictify_mt)
    else
        m.__newindex = strictify_mt.__newindex
        m.__index = strictify_mt.__index
    end
    return table
end

local function unstrictify(table)
    local m = getmetatable(table)

    if m == nil then
    elseif m == strictify_mt then
        setmetatable(table, nil)
    else
        if m.__newindex == strictify_mt.__newindex then
            m.__newindex = nil
        end
        if m.__index == strictify_mt.__index then
            m.__index = nil
        end
    end
    return table
end

return {
    strict      = strict,
    strictify   = strictify,
    unstrictify = unstrictify
}
