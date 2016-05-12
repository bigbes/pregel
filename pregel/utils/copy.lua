local strict = require('pregel.utils.strict')

local function shallow(orig)
    local orig_type = type(orig)
    local copy = orig
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in pairs(orig) do
            copy[orig_key] = orig_value
        end
    end
    return copy
end

local function deep(orig)
    local orig_type = type(orig)
    local copy = orig
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in pairs(orig) do
            copy[orig_key] = deep(orig_value)
        end
    end
    return copy
end

return strict.strictify({
    shallow = shallow,
    deep = deep
})
