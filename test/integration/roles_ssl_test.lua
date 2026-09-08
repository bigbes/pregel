--- The pregel roles over an SSL listener, discovery included.
--
-- Enterprise only: `transport: ssl` does not exist in Community, and there is
-- nothing to skip around -- the whole point is that the transport parameters
-- of iproto.listen reach net.box. Without them the peers dial an SSL listener
-- in plaintext, the listening side logs SSL_write() errors, and every instance
-- sits in 'connecting' for good.
--
-- The certificate is made here rather than committed: it is a throwaway
-- self-signed pair, and a repository is the wrong place for a private key even
-- when it guards nothing.

local t = require('luatest')

local fio       = require('fio')
local popen     = require('popen')
local tarantool = require('tarantool')

local Cluster = require('luatest.cluster')
local helper  = require('test.helpers.roles_cluster')

local g = t.group('integration.roles_ssl')

local ssl

--- Where `name` lives on PATH, or nil.
--
-- popen runs execve, not execvp: a bare 'openssl' is not looked up and the
-- child exits 2 with nothing on either stream, which reads exactly like the
-- tool failing rather than like it never running.
local function which(name)
    for dir in (os.getenv('PATH') or ''):gmatch('[^:]+') do
        local path = fio.pathjoin(dir, name)
        if fio.path.exists(path) then
            return path
        end
    end
    return nil
end

--- A self-signed certificate and its key, in a directory of their own.
local function make_certificate(openssl)
    local dir = fio.tempdir()
    local cert = fio.pathjoin(dir, 'server.crt')
    local key  = fio.pathjoin(dir, 'server.key')
    local ph = popen.new({
        openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-days', '1', '-subj', '/CN=localhost',
        '-keyout', key, '-out', cert,
    }, {stdout = 'devnull', stderr = 'pipe'})
    t.assert_not_equals(ph, nil, 'cannot run ' .. openssl)
    local complaint = ph:read({stderr = true, timeout = 60})
    local status = ph:wait()
    ph:close()
    t.assert_equals(status.exit_code, 0,
                    'openssl refused to make a certificate: ' ..
                    tostring(complaint))
    t.assert_equals(fio.path.exists(cert), true, 'no certificate was written')
    return {dir = dir, cert = cert, key = key}
end

g.before_all(function()
    t.skip_if(tarantool.package ~= 'Tarantool Enterprise',
              'transport: ssl is an Enterprise feature')
    local openssl = which('openssl')
    t.skip_if(openssl == nil, 'openssl is not on PATH')
    ssl = make_certificate(openssl)
end)

g.after_all(function()
    if ssl ~= nil then
        fio.rmtree(ssl.dir)
        ssl = nil
    end
end)

g.test_discovered_peers_keep_the_listener_transport = function()
    local c = Cluster:new(helper.config({
        autostart = true,
        ssl       = {cert = ssl.cert, key = ssl.key},
    }), helper.server_opts)
    c:start()

    -- Nothing but a working SSL handshake gets here: every message of the job
    -- crosses the same listener the config put the transport on.
    helper.wait_state(c, 'done', 30)

    local uris = c[helper.worker_name(1)]:exec(function(role)
        return require(role).get().workers
    end, {helper.WORKER_ROLE})
    t.assert_equals(#uris, helper.WORKER_COUNT)
    for _, uri in ipairs(uris) do
        t.assert_equals(type(uri), 'table',
                        'a discovered URI lost its parameters')
        t.assert_equals(uri.params.transport, 'ssl')
    end

    -- The listening side saw no plaintext: that is what a peer that dropped
    -- the transport looks like from here.
    t.assert_equals(c[helper.worker_name(1)]:grep_log('SSL_write'), nil)
end
