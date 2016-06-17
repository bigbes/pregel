local fun  = require('fun')
local log  = require('log')
local json = require('json')

local dup  = fun.duplicate

local function math_round(fnum)
    return (fnum % 1 >= 0.5) and math.ceil(fnum) or math.floor(fnum)
end

local function scalar_product(x, y)
    return fun.iter(x):zip(y):map(function(l, r) return l * r end):sum()
end

local function ConstantLearningRate_new(c)
    c = c or 0.01
    return setmetatable({
        c = c,
    }, {
        __index = {
            rate = function(self, iteration, parameters)
                return fun.range(1, #parameters):zip(dup(self.c)):tomap()
            end,
        }
    })
end

local function HingeLoss_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = scalar_product(x, parameters)
                if t * y < 1 then
                    result = fun.range(2, #x + 1)
                                :zip(x, dup(t))
                                :map(function(x_i, t) return -1 * t * x_i end)
                                :tomap()
                    result[1] = 1 - t * y
                else
                    result = fun.range(1, #parameters + 1):zip(dup(0)):tomap()
                end
                return result
            end,
        }
    })
end

local function L2_new()
    return setmetatable({}, {
        __index = {
            valueAndGradient = function(self, t, x, parameters)
                local result = nil
                local y = scalar_product(x, parameters)
                result = fun.range(2, #x + 1)
                            :zip(parameters)
                            :map(function(idx, p_i) return idx, p_i * 2 end)
                            :tomap()
                result[1] = scalar_product(parameters, parameters)
                result[1] = result[1] - (parameters[1] * parameters[1])
                return result
            end,
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

                local result = nil
                result = fun.iter(lossResult)
                            :zip(regularizerResult, dup(self.lambda))
                            :map(function(l, r, lmbd)
                    return l + lmbd * r
                end):totable()

                return result
            end,
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
                return self.loss:valueAndGradient(t, x, parameters)
            end,
            update = function(self, iteration, parameters, gradient)
                local rate = self.learningRate:rate(iteration, parameters)
                local updated = fun.iter(parameters):zip(rate, gradient)
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
        window_size = math.pow(2, 30)
    end
    return setmetatable({
        window_size = window_size,
        values = {},
        n = 0,
    }, {
        __index = {
            addValue = function(self, new)
                -- pop random element from table
                local isInserted = false
                if self.n == self.window_size then
                    table.remove(self.values, math.random(self.n))
                    self.n = self.n - 1
                end
                -- insert element in the right position
                for idx, val in ipairs(self.values) do
                    if val > new then
                        table.insert(self.values, new, idx)
                        isInserted = true
                        break
                    end
                end
                if not isInserted then
                    table.insert(self.values, new)
                end
                self.n = self.n + 1
            end,
            getPercentile = function(self, p)
                -- log.info('<PercentileCounter:getPercentile> %d, %f', self.n, p)
                local p = math_round(p * self.n / 100)
                -- log.info('<PercentileCounter:getPercentile> %d -> %s', p, tostring(self.values[p]))
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
