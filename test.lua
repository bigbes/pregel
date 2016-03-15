local fio = require('fio')
local log = require('log')
local yaml = require('yaml')

local pregel = require('pregel')
local xpcall_tb = require('pregel.utils').xpcall_tb

function preload_file(space)
    local f = fio.open('data/soc-Epinions1.txt', {'O_RDONLY'})
    local leftovers = ''
    local from = nil
    local filler = {}
    while true do
        local buf = leftovers .. f:read(4096)
        if #buf == 0 then break end
        for line in buf:gmatch("[^\n]+\n") do
            local n1, n2 = line:match("^(%d+)[^\n]*(%d+)")
            n1, n2 = tonumber(n1), tonumber(n2)
            if n1 ~= nil and n2 ~= nil then
                from = from or n1
                if from == n1 then
                    table.insert(filler, n2)
                else
                    space:insert{from, 0, math.random(1000000), filler}
                    from = n1
                    filler = { n2 }
                end
            end
        end
        local ed = buf:find("\n[^\n]*$")
        leftovers = buf:sub(ed + 1)
    end
end

box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

local function f1_process(self)
    local function inform_neighbors(val)
        self:pairs_edges():each(function(neighbor)
            self:send_message(neighbor, val)
        end)
    end
    if self.superstep == 1 then
        inform_neighbors(self.value)
    elseif self.superstep < 30 then
        local modified = false
        for msg in self:pairs_messages() do
            if self.value < msg then
                self.value = msg
                modified = true
            end
        end
        if modified then
            inform_neighbors(self.value)
        end
    end
    self:set_halt()
end

local a = pregel.new('test', f1_process)
xpcall_tb(a.run, a)
for k = 0, 9 do
    log.info('%s', require('yaml').encode(
        a.space:select({}, {offset = k, limit = 1})
    ))
end
os.exit(0)

local console = require('console')
console.start()
