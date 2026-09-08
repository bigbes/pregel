--- Bootstrap for a pregel instance under luatest.Server.
--
-- Modelled on luatest's own server_instance.lua: the same TARANTOOL_LISTEN /
-- TARANTOOL_WORKDIR environment and the same _G.ready convention, which is
-- what Server:wait_until_ready() blocks on.
--
-- Every instance is both a possible master and a possible worker -- which of
-- the two it becomes is decided later, by the test calling
-- server:exec(...) -- so both grant helpers run here. No universe grant: the
-- helpers hand out execute on lua_call for their own entry points only.

local fio  = require('fio')
local json = require('json')

local work_dir = os.getenv('TARANTOOL_WORKDIR')
local listen   = os.getenv('TARANTOOL_LISTEN')

assert(work_dir ~= nil, 'TARANTOOL_WORKDIR is not set')
assert(listen ~= nil, 'TARANTOOL_LISTEN is not set')

fio.mktree(work_dir)

box.cfg{
    work_dir  = work_dir,
    listen    = listen,
    -- Nothing here outlives the test, and a WAL would only make it slower.
    wal_mode  = 'none',
    log_level = tonumber(os.getenv('TARANTOOL_LOG_LEVEL')) or 5,
    memtx_memory = 128 * 1024 * 1024,
}

-- The repository root, so the instance can require pregel from the checkout it
-- was started from rather than from an installed rock.
local root = os.getenv('PREGEL_ROOT')
if root ~= nil then
    package.path = fio.pathjoin(root, '?.lua') .. ';' ..
                   fio.pathjoin(root, '?/init.lua') .. ';' .. package.path
end

-- The test harness drives this instance with Server:exec(), which is eval on
-- the far side and so needs a privileged user. luatest's own bootstrap gives
-- that to `guest`; doing the same here would make every privilege claim in the
-- tests vacuous, since guest is also the user the pregel instances connect to
-- each other as. So the harness gets a user of its own and guest keeps only
-- what the grant helpers below hand out.
local credentials = os.getenv('TARANTOOL_CREDENTIALS')
if credentials ~= nil then
    credentials = json.decode(credentials)
    assert(type(credentials.user) == 'string')
    assert(credentials.user ~= 'guest',
           'the luatest user must not be guest: see the comment above')
    if credentials.user ~= 'admin' then
        box.schema.user.create(credentials.user, {if_not_exists = true})
        box.schema.user.grant(credentials.user, 'super', nil, nil,
                              {if_not_exists = true})
        if next(box.space._user.index.name:get(credentials.user).auth) == nil then
            box.schema.user.passwd(credentials.user,
                                   credentials.password or '')
        end
    end
end

local worker = require('pregel.worker')
local master = require('pregel.master')

-- Requiring the modules is what publishes _G.pregel.worker / _G.pregel.master;
-- these two make them reachable by the guest user other instances connect as.
worker.grant('guest')
master.grant('guest')

-- Handy for a test that wants to poke at the instance from outside.
_G.pregel_worker = worker
_G.pregel_master = master

_G.ready = true
