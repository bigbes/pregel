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

local log = require('log')

local is_callable = require('pregel.utils').is_callable
local deepcopy    = require('pregel.utils.copy').deep

local MASTER_DELIVER = 'pregel.master.deliver'

local aggregator_mt = {
    __index = {
        --- Queue this value on every worker, to go out with the next flush.
        inform_workers = function(self)
            for _, bucket in ipairs(self.pregel.mpool.buckets) do
                bucket:put('aggregator.inform', {self.name, self.value})
            end
        end,
        --- Report this worker's copy to the master.
        inform_master = function(self)
            -- conn:call against the master's registry, not conn:eval: eval
            -- needs a universe execute grant, a call needs only lua_call on
            -- this one name.
            self.pregel.master:call(MASTER_DELIVER, {
                'aggregator.inform', {self.name, self.value}
            })
        end,
        make_default = function(self)
            if type(self.default) == 'function' then
                self.value = self.default()
            else
                -- A copy: a table default would otherwise be shared with, and
                -- mutated by, every superstep that followed.
                self.value = deepcopy(self.default)
            end
        end,
        merge_master = function(self, value)
            self.value = self.merge(self.value, value)
        end
    },
    --- aggregator()        -> current value
    --  aggregator(value)   -> contribute value
    __call = function(self, value)
        if value == nil then
            return self.value
        end
        self.value = self.reduce(self.value, value)
    end
}

--- opts.default  -- starting value, or a function returning one
--  opts.reduce   -- callable(accumulator, contribution) -> accumulator
--  opts.merge    -- callable(accumulator, worker_value) -> accumulator
--                   (defaults to reduce)
--  opts.internal -- true for the aggregators pregel keeps for itself
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
        default    = opts.default,
        pregel     = pregel,
    }, aggregator_mt)
end

return {
    new = aggregator_new
}
