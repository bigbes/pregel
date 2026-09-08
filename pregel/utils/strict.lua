--[[--
-- Turn a module table into one that refuses silent typos.
--
--   local m = strict.strictify({ new = new })
--   m.new()      -- fine
--   m.nwe()      -- error: variable 'nwe' is not declared
--
-- Reading a key the table does not hold is a mistake in every module here --
-- a renamed or removed helper otherwise comes back as nil and fails much later
-- as "attempt to call a nil value" at the call site, which says nothing about
-- which name was wrong. Writes are allowed and register the key, so a module
-- can still grow fields after strictify().
--
-- `unstrictify` restores plain table behaviour.
--
-- @module pregel.utils.strict
--]]--

-- Per-table sets of declared keys, keyed weakly so a dropped module table does
-- not keep its key set alive (the previous implementation keyed on the address
-- from tostring(), which both leaked and collided once an address was reused).
local declared = setmetatable({}, { __mode = 'k' })

local function declared_of(t)
    local set = declared[t]
    if set == nil then
        set = {}
        declared[t] = set
    end
    return set
end

local strictify_mt
strictify_mt = {
    __index = function(t, n)
        if not declared_of(t)[n] then
            error("variable '" .. tostring(n) .. "' is not declared", 2)
        end
        return rawget(t, n)
    end,
    __newindex = function(t, n, v)
        declared_of(t)[n] = true
        rawset(t, n, v)
    end
}

--- Make reading an undeclared key of `t` an error.
--
-- The table is modified in place rather than copied, so every reference to it
-- becomes strict at once. A table that already has a metatable keeps it and
-- only has __index/__newindex replaced -- which means strictify() overrides an
-- existing __index, and a table that needs one cannot be strictified.
--
-- @param t table to protect
-- @return the same table
-- @function strictify
local function strictify(t)
    -- Everything the table already holds counts as declared; __index only ever
    -- fires for keys that are absent.
    local set = declared_of(t)
    for k in pairs(t) do
        set[k] = true
    end

    local mt = getmetatable(t)
    if mt == nil then
        setmetatable(t, strictify_mt)
    elseif mt ~= strictify_mt then
        mt.__index = strictify_mt.__index
        mt.__newindex = strictify_mt.__newindex
    end
    return t
end

--- Undo strictify(): reading a missing key is a plain nil again.
--
-- Safe on a table that was never strictified, and on one whose metatable came
-- from elsewhere -- only the hooks this module installed are removed.
--
-- @param t table
-- @return the same table
-- @function unstrictify
local function unstrictify(t)
    local mt = getmetatable(t)
    if mt == strictify_mt then
        setmetatable(t, nil)
    elseif mt ~= nil then
        if mt.__index == strictify_mt.__index then
            mt.__index = nil
        end
        if mt.__newindex == strictify_mt.__newindex then
            mt.__newindex = nil
        end
    end
    declared[t] = nil
    return t
end

return {
    strictify   = strictify,
    unstrictify = unstrictify
}
