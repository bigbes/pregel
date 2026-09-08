--- box.cfg for unit tests that need real spaces.
--
-- The queue and worker spaces are ordinary Tarantool spaces, so the tests that
-- exercise them have to run inside a configured instance. luatest runs a whole
-- suite in one process, so box.cfg happens once and every group shares it;
-- tests keep themselves apart by using distinct space names and dropping what
-- they create.

local fio = require('fio')

local helper = {}

--- Configure box in a scratch directory under test/var.
--
-- No WAL: nothing here needs to survive the process, and writing one makes the
-- suite an order of magnitude slower.
--
-- box.cfg is a function until it has been called and a table afterwards, which
-- is the only reliable way to tell whether something already configured this
-- process -- luatest configures box itself when a test needs a box, and
-- calling box.cfg again would fail on the options it cannot change at runtime.
function helper.cfg()
    if type(box.cfg) ~= 'function' then
        return
    end
    local dir = fio.pathjoin(fio.cwd(), 'test', 'var', 'unit')
    fio.rmtree(dir)
    fio.mktree(dir)
    -- No `log` option on purpose: luatest has already pointed the logger at
    -- its own pipe with log.cfg{}, and box refuses to move `log` afterwards
    -- ("Can't set option 'log' dynamically"), which would fail every test that
    -- needs a space.
    box.cfg{
        memtx_dir = dir,
        wal_dir   = dir,
        wal_mode  = 'none',
        log_level = 4,
    }
end

return helper
