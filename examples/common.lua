--- The bits every example app module needs.
--
-- An example is started two ways: by `tt` from the example's own directory,
-- and by the test suite from the repository root. Nothing in an app module can
-- therefore depend on the process's working directory -- and it cannot depend
-- on the cluster config's either, since tt gives every instance a working
-- directory of its own under var/lib.
--
-- What is stable in both cases is where the app module itself was found: the
-- LUA_PATH that made `require` work names an absolute directory. So a path
-- written into roles_cfg.app_cfg is relative to the example directory, and
-- resolved here against the module that asked for it.

local fio = require('fio')

local M = {}

--- The absolute directory of the file `level` frames up the stack.
--
-- Called at the top of an app module, the default level is that module's own
-- chunk. A source that is not a file -- a chunk loaded from a string, which is
-- what an app module pasted into a console would be -- has no directory to
-- resolve against, and saying so here beats resolving paths against the
-- process's working directory by accident.
function M.here(level)
    local info = debug.getinfo(level or 2, 'S')
    local source = info and info.source or ''
    if source:sub(1, 1) ~= '@' then
        error('examples.common.here: caller was not loaded from a file (' ..
              tostring(info and info.short_src) .. '); pass an absolute path ' ..
              'in app_cfg instead', 2)
    end
    return fio.abspath(fio.dirname(source:sub(2)))
end

--- Resolve one app_cfg path against the example's directory.
function M.resolve(dir, path, what)
    if type(path) ~= 'string' then
        error(string.format(
            'examples.common.resolve: app_cfg.%s must be a path, got %s',
            tostring(what or 'path'), type(path)), 2)
    end
    if path:sub(1, 1) == '/' then
        return path
    end
    return fio.abspath(fio.pathjoin(dir, path))
end

--- app_cfg as the app modules want to see it: never nil, and complaining by
-- name about a missing key rather than about a nil index three calls later.
function M.cfg(app_cfg, required)
    app_cfg = app_cfg or {}
    for _, key in ipairs(required or {}) do
        if app_cfg[key] == nil then
            error(string.format(
                "this example needs roles_cfg app_cfg.%s, which the cluster " ..
                "config does not set", key), 2)
        end
    end
    return app_cfg
end

return M
