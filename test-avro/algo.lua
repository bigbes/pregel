local fun = require('fun')

local function ConstantLearningRate_new(c)
    c = c or 0.01
    return setmetatable({
        c = c,
    }, {
        __index = {
            rate = function(self, iteration, parameters)
                return fun.range(1, #parameters):zip(
                    fun.duplicate(self.c)
                ):tomap()
            end,
        }
    })
end

local function common_dot(self, x, y)
    return fun.iter(x):zip(y):reduce(function(acc, l, r)
        return acc + l * r
    end, 0.0)
end

local function HingeLoss_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = self:dot(x, parameters)
                if t * y < 1 then
                    result = fun.range(2, #result + 1)
                                :zip(x)
                                :zip(fun.duplicate(t))
                                :map(function(x_i, t) return -1 * t * x_i end)
                                :tomap()
                    result[1] = 1 - t * y
                else
                    result = fun.range(1, #result + 1)
                                :zip(fun.duplicate(0.0))
                                :tomap()
                end
                return result
            end,
            dot = common_dot
        }
    })
end

local function L2_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = self:dot(x, parameters)
                result = fun.range(2, #result + 1)
                            :zip(parameters)
                            :map(function(idx, p_i) return idx, p_i * 2 end)
                            :tomap()
                result[1] = self:dot(parameters, parameters)
                result[1] = result[1] - (parameters[0] * parameters[0])
                return result
            end,
            dot = common_dot
        }
    })
end

local function RegularizedLoss_new(loss, regularizer, lambda)
    return setmetatable({
        loss        = loss,
        regularizer = regularizer,
        lambda      = lambda
    }, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local lossResult         = self.loss:valueAndGradient(t, x, parameters)
                local regularizerResult  = self.regularizer:valueAndGradient(t, x, parameters)

                local result = fun.iter(lossResult):zip(regularizerResult)
                                                   :zip(fun.duplicate(self.lambda))
                                                   :map(function(l, r, lambda)
                    return l + lambda * r
                end):totable()

                return result
            end,
            dot = common_dot
        }
    })
end

local function GradientDescent_new(loss, lr)
    return setmetatable({
        -- Optimized Function
        loss         = RegularizedLoss_new(HingeLoss_new(), L2_new(), 0.0),
        -- Learning Rate
        learningRate = ConstantLearningRate_new(0.00005),
    }, {
        __index = {
            lossAndGradient = function(self, t, x, parameters)
                self.loss:valueAndGradient(t, x, parameters)
            end,
            update = function(self, iteration, parameters, gradient)
                local rate = self.learningRate:rate(iteration, parameters)
                local updated = fun.iter(parameters):zip(rate):zip(gradient)
                                   :map(function(p, r, g) return p - r * g end)
                                   :totable()
                return updated
            end,
            initialize = function(self, dim)
                local parameters = fun.range(dim):map(
                    function() return 2 * math.random() - 1 end
                ):totable()
                return parameters
            end,
        }
    })
end

local function PercentileCounter_new(window_size)
    if window_size == nil then
        window_size = math.pow(2, 31)
    end
    return setmetatable({
        window_size = window_size,
        values = {},
        n = 0,
    }, {
        __index = {
            addValue = function(self, new)
                -- pop random element from table
                if self.n == self.window_size then
                    table.remove(self.values, math.random(self.n))
                    self.n = self.n - 1
                end
                -- insert element in the right position
                for idx, val in ipairs(self.values) do
                    if val > new then
                        table.insert(self, new, idx)
                        self.n = self.n + 1
                        break
                    end
                end
            end,
            getPercentile = function(self, p)
                p = (p * self.n / 100)
                if p % 1 >= 0.5 then
                    p = math.ceil(p)
                else
                    p = math.floor(p)
                end
                return self.values[p]
            end,
            getN = function(self)
                return self.n
            end
        }
    })
end

return {
    GradientDescent   = GradientDescent_new,
    PercentileCounter = PercentileCounter_new
}
