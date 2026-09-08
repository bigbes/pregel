--- An app module that records what the roles hand it.
--
-- The job context is the half of an app module's input that does not come from
-- app_cfg -- the job name, the login pregel connects as, this instance's name
-- and the module's own directory -- and it reaches an app at three call sites.
-- Each one records what it was given, under _G, where a test can read it back
-- over server:exec().
--
-- The graph is empty on purpose: what is under test is what the roles pass,
-- and every one of the three call sites runs during apply(), before a job has
-- to move at all.

local loader = require('pregel.loader')

-- Recorded per call site, so a role that hands the context to one of them and
-- not to another is not covered by the other two.
rawset(_G, 'pregel_test_job_context', {})

local function record(where, app_cfg, context)
    _G.pregel_test_job_context[where] = {
        app_cfg = app_cfg,
        context = context,
        -- A context that arrives as something other than a table would fail
        -- the assertions below in a way that reads like a missing field.
        type    = type(context),
    }
end

local app = {}

function app.obtain_name(vertex)
    return vertex.name
end

function app.compute(self)
    self:vote_halt(true)
end

function app.worker_context(app_cfg, context)
    record('worker_context', app_cfg, context)
    return {job = context and context.name}
end

function app.worker_preload(instance, app_cfg, context)
    record('worker_preload', app_cfg, context)
    return loader.new(instance, function(self)
        self:flush()
    end)
end

function app.master_preload(instance, app_cfg, context)
    record('master_preload', app_cfg, context)
    return loader.new(instance, function(self)
        self:flush()
    end)
end

return app
