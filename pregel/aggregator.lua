--- Aggregators: values every vertex can contribute to and read back.
--
-- Each worker keeps its own copy. A superstep ends with every worker reporting
-- its copy to the master (inform_master), the master merging them, and the
-- merged value going back out to the workers (inform_workers) -- so a vertex
-- reads the value the whole graph produced in the previous superstep.
--
-- `reduce` folds one vertex's contribution into the worker's copy; `merge`
-- folds one worker's copy into the master's. They are usually the same
-- function, and merge defaults to reduce.
--
-- A worker's aggregator therefore holds two values, not one:
--
--   value  -- what the vertices of the superstep now running have contributed
--             so far; this is what goes to the master at the end of it
--   global -- what the master merged out of every worker at the end of the
--             *previous* superstep; this is what a vertex reads
--
-- Keeping them apart is what makes a summing aggregator sum. With a single
-- value the merged global is still sitting in the accumulator when the next
-- superstep starts contributing to it, so every worker reports the global
-- again and the master adds it once per worker: four workers turned a count of
-- 2000 into 10000 and then 42000. See receive_global below.
--
-- @module pregel.aggregator

local log = require('log')

local is_callable = require('pregel.utils').is_callable
local deepcopy    = require('pregel.utils.copy').deep

local MASTER_DELIVER = 'pregel.master.deliver'

local aggregator_mt = {
    __index = {
        --- Queue this value on every worker, to go out with the next flush.
        --
        -- Called on the master's copy after it has merged every worker's, so
        -- what is queued is the merged value each worker will read back as its
        -- `global`.
        --
        -- @function inform_workers
        inform_workers = function(self)
            for _, bucket in ipairs(self.pregel.mpool.buckets) do
                bucket:put('aggregator.inform', {self.name, self.value})
            end
        end,
        --- Report this worker's copy to the master.
        --
        -- Blocks on the master's reply, so it must run in a fiber that may
        -- yield.
        --
        -- @function inform_master
        inform_master = function(self)
            -- conn:call against the master's registry, not conn:eval: eval
            -- needs a universe execute grant, a call needs only lua_call on
            -- this one name.
            self.pregel.master:call(MASTER_DELIVER, {
                'aggregator.inform', {self.name, self.value}
            })
        end,
        --- Reset the accumulator to the configured default.
        --
        -- A function default is called afresh; anything else is deep-copied.
        --
        -- @function make_default
        make_default = function(self)
            if type(self.default) == 'function' then
                self.value = self.default()
            else
                -- A copy: a table default would otherwise be shared with, and
                -- mutated by, every superstep that followed.
                self.value = deepcopy(self.default)
            end
        end,
        --- Fold one worker's reported copy into the master's accumulator.
        --
        -- Runs on the master, once per worker per superstep, through `merge`
        -- rather than `reduce`.
        --
        -- @param value the worker's copy
        -- @function merge_master
        merge_master = function(self, value)
            self.value = self.merge(self.value, value)
        end,
        --- A worker takes delivery of the master's merged value.
        --
        -- Which is also the moment the superstep that produced it is over, so
        -- it is the moment to clear the accumulator: from here until the end
        -- of the next superstep, `value` holds nothing but what that
        -- superstep's own vertices contribute.
        -- @param value the master's merged value
        -- @function receive_global
        receive_global = function(self, value)
            self.global = value
            self:make_default()
        end,
        --- What vertex:get_aggregation() answers: the whole graph's value from
        -- the previous superstep, not this worker's running total for the
        -- current one -- which would make what a vertex reads depend on how
        -- many vertices of its own shard happened to be computed before it.
        --
        -- In superstep 1 nothing has been merged yet, so this is the default.
        --
        -- @return the merged value
        -- @function get_global
        get_global = function(self)
            return self.global
        end,
    },
    --- aggregator()        -> current value
    --  aggregator(value)   -> contribute value
    --
    -- The read is of the local accumulator, not the merged global -- a vertex
    -- wants get_global() and reaches it through vertex:get_aggregation().
    -- Contributing nil is indistinguishable from a read, so an aggregator
    -- cannot be given one.
    --
    -- @param value the contribution, or nil to read
    -- @return the accumulator when called with no argument, nothing otherwise
    -- @function __call
    __call = function(self, value)
        if value == nil then
            return self.value
        end
        self.value = self.reduce(self.value, value)
    end
}

--- Create an aggregator for one instance, master or worker.
--
-- opts.default  -- starting value, or a function returning one
-- opts.reduce   -- callable(accumulator, contribution) -> accumulator
--                  (defaults to last-write-wins)
-- opts.merge    -- callable(accumulator, worker_value) -> accumulator
--                  (defaults to reduce)
-- opts.internal -- true for the aggregators pregel keeps for itself
--
-- @param name string the instance registers it under
-- @param pregel the owning master or worker instance
-- @param opts optional table as above
-- @return the aggregator object
-- @raise when reduce or merge is given and is not callable
-- @function new
local function aggregator_new(name, pregel, opts)
    opts = opts or {}
    local internal = opts.internal or false
    local reduce = opts.reduce or (function(_, v) return v end)
    assert(is_callable(reduce), 'options.reduce must be callable')
    -- `opts.merge or opts.reduce` left merge nil whenever neither was given,
    -- and the assert below then rejected a perfectly ordinary aggregator.
    local merge = opts.merge or reduce
    assert(is_callable(merge), 'options.merge must be callable')

    log.verbose('<aggregator, %s> creating new aggregator', name)

    return setmetatable({
        name       = name,
        reduce     = reduce,
        merge      = merge,
        internal   = internal,
        value      = opts.default,
        -- Read by every vertex of superstep 1, before any master has merged
        -- anything: the default is the only honest answer there.
        global     = opts.default,
        default    = opts.default,
        pregel     = pregel,
    }, aggregator_mt)
end

return {
    new = aggregator_new
}
