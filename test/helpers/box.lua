--- box.cfg for unit tests that need real spaces.
--
-- The queue and worker spaces are ordinary Tarantool spaces, so the tests that
-- exercise them have to run inside a configured instance. luatest runs a whole
-- suite in one process, so box.cfg happens once and every group shares it;
-- tests keep themselves apart by using distinct space names and dropping what
-- they create.

local fio = require('fio')

local helper = {}

local DIR = fio.pathjoin(fio.cwd(), 'test', 'var', 'unit')

--- Configure box in a scratch directory under test/var.
--
-- No WAL: nothing here needs to survive the process, and writing one makes the
-- suite an order of magnitude slower.
--
-- box.cfg is a function until it has been called and a table afterwards, which
-- is how to tell whether something already configured this process.
function helper.cfg()
    if type(box.cfg) ~= 'function' then
        return
    end
    fio.rmtree(DIR)
    fio.mktree(DIR)
    -- No `log` option on purpose: luatest has already pointed the logger at
    -- its own pipe with log.cfg{}, and box refuses to move `log` afterwards
    -- ("Can't set option 'log' dynamically"), which would fail every test that
    -- needs a space.
    box.cfg{
        memtx_dir = DIR,
        wal_dir   = DIR,
        wal_mode  = 'none',
        log_level = 4,
    }
end

--- Start listening on a unix socket and return its net.box URI.
--
-- mpool decides whether a bucket is local by comparing the peer uuid from the
-- connection greeting against box.info.uuid, so a unit test needs an address
-- of its own to point a bucket at.
function helper.listen_uri()
    helper.cfg()
    if box.cfg.listen == nil then
        box.cfg{listen = 'unix/:' .. fio.pathjoin(DIR, 'unit.sock')}
    end
    return box.cfg.listen
end

return helper
