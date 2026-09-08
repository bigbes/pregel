--- Resolving the user pregel connects as out of the `credentials` section.
--
-- The roles are not told a login any more: they look for the user the cluster
-- config marks with the credentials role `pregel`, the way the framework
-- checks a vshard storage's user for the semi-default `sharding` role. Every
-- instance of a job resolves this on its own, so the interesting cases are the
-- ones where the config does not single out one user -- two instances that
-- read it differently would authenticate to each other as different users.
--
-- The cluster config is a stub here, and the resolution takes it as an
-- argument for that reason: what is under test is the reading of a document,
-- and the documents that are wrong are exactly the ones no cluster would
-- start with. test/integration/roles_test.lua runs the same code against a
-- real config framework.

local t = require('luatest')

local common = require('pregel.roles.common')

local g = t.group('integration.roles_credentials')

local ROLE = 'pregel.roles.worker'

--- Something with the one method common.pregel_user uses.
--
-- config:get() takes a path as an array of keys, and answers nil for a path
-- that is not there -- measured on CE 3.9 and EE 3.7 against a real config
-- (`credentials.users.nosuch.password` answers nil rather than raising).
local function fake_config(document)
    return {
        get = function(_, path)
            local node = document
            for _, key in ipairs(path) do
                if type(node) ~= 'table' then
                    return nil
                end
                node = node[key]
            end
            return node
        end,
    }
end

local function credentials(users, roles)
    return fake_config({credentials = {users = users, roles = roles}})
end

-------------------------------------------------------------------------------

g.test_the_user_carrying_the_role_is_the_pregel_user = function()
    local user, password = common.pregel_user(ROLE, credentials({
        replicator  = {password = 'r', roles = {'replication'}},
        pregel_peer = {password = 'secret', roles = {'pregel'}},
    }))
    t.assert_equals(user, 'pregel_peer')
    t.assert_equals(password, 'secret')
end

-- Credentials roles nest, and a deployment that wraps pregel's role in one of
-- its own has still marked that user. The framework reads the sharding role
-- the same way.
g.test_the_role_is_found_through_another_role = function()
    local user = common.pregel_user(ROLE, credentials({
        graph = {password = 'secret', roles = {'graph_traffic'}},
    }, {
        graph_traffic = {roles = {'pregel'}},
    }))
    t.assert_equals(user, 'graph')
end

-- Two roles that name each other is a configuration the credentials applier
-- refuses -- but this runs before it has had to, and a walk with no guard
-- would hang the instance's whole startup instead of reporting anything.
g.test_a_cycle_in_the_roles_is_not_a_hang = function()
    t.assert_error_msg_contains(
        'no user in the cluster config has the credentials role',
        common.pregel_user, ROLE, credentials({
            someone = {password = 'x', roles = {'a'}},
        }, {
            a = {roles = {'b'}},
            b = {roles = {'a'}},
        }))
end

-- Nothing else names the login, so this is not "a default was used" but "the
-- job cannot start". The message has to say what to write and where.
g.test_no_user_with_the_role_is_refused = function()
    local ok, err = pcall(common.pregel_user, ROLE, credentials({
        replicator = {password = 'r', roles = {'replication'}},
    }))
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err),
                          "pregel.roles.worker: no user in the cluster " ..
                          "config has the credentials role 'pregel'")
    t.assert_str_contains(tostring(err), "add 'roles: [pregel]' to the user")
    t.assert_str_contains(tostring(err), 'credentials.users')
end

-- An empty credentials section is the same failure and must read the same way:
-- a config that says nothing about the user is not a config that says guest.
g.test_an_empty_credentials_section_is_refused = function()
    t.assert_error_msg_contains(
        "no user in the cluster config has the credentials role 'pregel'",
        common.pregel_user, ROLE, fake_config({}))
end

-- Every instance resolves this for itself, so a config that marks two users is
-- one where two instances may pick different logins -- and a worker that
-- authenticates as a user the others did not expect fails at its first write,
-- not at apply time. Both names are in the message, in a fixed order.
g.test_two_users_with_the_role_are_refused = function()
    local ok, err = pcall(common.pregel_user, ROLE, credentials({
        zeta  = {password = 'z', roles = {'pregel'}},
        alpha = {password = 'a', roles = {'pregel'}},
    }))
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err),
                          "2 users in the cluster config have the " ..
                          "credentials role 'pregel' ('alpha', 'zeta')")
    t.assert_str_contains(tostring(err), 'exactly one of them')
end

-- The password is what the peers are authenticated with; without one the
-- connection is refused by every peer, which reads as "the cluster is down"
-- rather than as "this user has no password".
g.test_a_user_without_a_password_is_refused = function()
    local ok, err = pcall(common.pregel_user, ROLE, credentials({
        pregel_peer = {roles = {'pregel'}},
    }))
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err),
                          "the user 'pregel_peer' has the credentials role " ..
                          "'pregel' but no password")
    t.assert_str_contains(tostring(err),
                          'set credentials.users.pregel_peer.password')
end

-- An empty password is a real configuration -- it is what a cluster on a
-- trusted socket writes -- and telling it from an absent one is the whole
-- point of testing for nil rather than for falsiness.
g.test_an_empty_password_is_a_password = function()
    local user, password = common.pregel_user(ROLE, credentials({
        pregel_peer = {password = '', roles = {'pregel'}},
    }))
    t.assert_equals(user, 'pregel_peer')
    t.assert_equals(password, '')
end

-- The messages reach whoever wrote the YAML as a config alert, so they carry
-- no '/Users/.../common.lua:123:' in front of them.
g.test_the_messages_carry_no_source_position = function()
    local _, err = pcall(common.pregel_user, ROLE, fake_config({}))
    t.assert_equals(tostring(err):sub(1, #ROLE), ROLE)
end
