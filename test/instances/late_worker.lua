--- An instance that is up long before it serves pregel.
--
-- The window pregel-ib4 is about: tt forks the instances of a cluster in
-- whatever order it likes and nothing sequences the role appliers, so a peer
-- regularly accepts connections -- with its user and its grants already in
-- place -- while pregel/worker.lua has not been required there yet. This
-- script is that peer, with the delay made explicit instead of raced for.
--
-- Two shapes of "not yet", chosen by PREGEL_LATE_MODE:
--
--   * proc  (default) -- the lua_call grant is there from the start and
--     _G.pregel.worker appears after the delay. A caller sees ER_NO_SUCH_PROC.
--   * grant -- _G.pregel.worker is there from the start and the grant appears
--     after the delay. A caller sees ER_ACCESS_DENIED.
--
-- PREGEL_LATE_DELAY is the delay in seconds; a delay longer than the test's
-- own timeout is how the give-up path is tested.
--
-- Nothing here requires pregel: publishing the registry entry is a two-line
-- job, and requiring the real module would defeat the point by publishing it
-- at load time.

local fio   = require('fio')
local fiber = require('fiber')

local work_dir = os.getenv('TARANTOOL_WORKDIR')
local listen   = os.getenv('TARANTOOL_LISTEN')

assert(work_dir ~= nil, 'TARANTOOL_WORKDIR is not set')
assert(listen ~= nil, 'TARANTOOL_LISTEN is not set')

fio.mktree(work_dir)

box.cfg{
    work_dir  = work_dir,
    listen    = listen,
    wal_mode  = 'none',
    log_level = tonumber(os.getenv('TARANTOOL_LOG_LEVEL')) or 5,
}

-- The harness drives this instance with Server:exec(), which is eval on the far
-- side and needs a privileged user. It must not be `guest`: guest is what the
-- pool under test connects as, and the whole point of the 'grant' mode is that
-- guest holds nothing until the delay has passed.
box.schema.user.create('luatest', {password = 'luatest', if_not_exists = true})
box.schema.user.grant('luatest', 'super', nil, nil, {if_not_exists = true})

local RPC_DELIVER = 'pregel.worker.deliver'

local mode  = os.getenv('PREGEL_LATE_MODE') or 'proc'
local delay = tonumber(os.getenv('PREGEL_LATE_DELAY')) or 1

local function publish()
    rawset(_G, 'pregel', rawget(_G, 'pregel') or {})
    -- The same two messages the real deliver answers without an instance; a
    -- probe only ever sends 'ping'.
    _G.pregel.worker = {
        deliver = function(_, msg)
            if msg == 'ping' then
                return true
            end
            error('late_worker: unexpected message ' .. tostring(msg))
        end,
    }
end

local function allow()
    box.schema.user.grant('guest', 'execute', 'lua_call', RPC_DELIVER,
                          {if_not_exists = true})
end

if mode == 'proc' then
    allow()
else
    publish()
end

fiber.create(function()
    fiber.sleep(delay)
    if mode == 'proc' then
        publish()
    else
        allow()
    end
    _G.serving = true
end)

_G.ready = true
