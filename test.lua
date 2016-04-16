local fio = require('fio')
local log = require('log')
local json = require('json')
local yaml = require('yaml')

local pregel      = require('pregel')
local xpcall_tb   = require('pregel.utils').xpcall_tb
local defaultdict = require('pregel.utils.collections').defaultdict

function preload_file(space)
    local graph = defaultdict(function(key)
        local tbl = {key, false, math.random(1000000), {}}
        setmetatable(tbl,    { __serialize = 'seq' })
        setmetatable(tbl[4], { __serialize = 'map' })
        return tbl
    end)

    local f = fio.open('data/soc-Epinions1.txt', {'O_RDONLY'})
    local leftovers = ''

    while true do
        local buf = leftovers .. f:read(4096)
        if #buf == 0 then break end
        for line in buf:gmatch("[^\n]+\n") do
            local n1, n2 = line:match("^(%d+)\t*(%d+)")
            n1, n2 = tonumber(n1), tonumber(n2)
            if n1 ~= nil and n2 ~= nil then
                graph[n1][4][n2] = true
                graph[n2][4][n1] = true
            end
        end
        local ed = buf:find("\n[^\n]*$")
        leftovers = buf:sub(ed + 1)
    end

    for _, tuple in pairs(graph) do
        local tbl = {}
        for k, v in pairs(tuple[4]) do
            table.insert(tbl, k)
        end
        table.sort(tbl)
        tuple[4] = tbl
        setmetatable(tbl, { __serialize = 'seq' })
        space:insert(tuple)
    end
end

box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

local function graph_max_process(self)
    local function inform_neighbors(val)
        for _, neighbor in pairs(self.__edges) do
            self:send_message(neighbor, val)
        end
    end
    if self.superstep == 1 then
        inform_neighbors(self:get_value())
    elseif self.superstep < 30 then
        local modified = false
        for _, msg in self:pairs_messages() do
            if self:get_value() < msg then
                self:set_value(msg)
                modified = true
            end
        end
        if modified then
            inform_neighbors(self:get_value())
        end
    end
    self:vote_halt(true)
end

local a = pregel.new('test', graph_max_process)
xpcall_tb(a.run, a)
os.exit(0)

local console = require('console')
console.start()
