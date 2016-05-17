local is_callable = require('pregel.utils').is_callable

local aggregator_mt = {
    __index = {
        inform_workers = function(self)
            for _, bucket in ipairs(self.pregel.mpool.buckets) do
                bucket:put('aggregator.inform', {self.name, self.value})
            end
        end,
        inform_master = function(self)
            self.pregel.master:eval(
                'return require("pregel.master").deliver(...)',
                'aggregator.inform', {self.name, self.value}
            )
        end,
        make_default = function(self)
            self.value = self.default
        end,
        merge_master = function(self, value)
            self.value = self.merge(self.value, value)
        end
    },
    __call = function(self, val)
        if type(val) == 'nil' then
            return self.value
        end
        self.value = self.reduce(self.value, val)
    end
}

local function aggregator_new(name, pregel, opts)
    opts = opts or {}
    local internal = opts.internal or false
    local reduce = opts.reduce or (function(k, v) return v end)
    assert(is_callable(reduce), 'options.reduce must be callable')
    local merge = opts.merge or (function(k, v) return v end)
    assert(is_callable(merge), 'options.merge must be callable')

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
