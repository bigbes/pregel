<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>
<a href="https://travis-ci.org/tarantool/pregel">
	<img src="https://travis-ci.org/tarantool/pregel.png?branch=master" align="right">
</a>

# Large scale graph processing based on Tarantool

Based on [Pregel whitepaper](http://kowshik.github.io/JPregel/pregel_paper.pdf).

As 'abstract' says:

> Many practical computing problems concern large graphs. Standard examples
> include the Web graph and various social networks. The scale of these
> graphs - in some cases billions of vertices, trillions of edges — poses
> challenges to their efficient processing. In this paper we present a
> computational model suitable for this task. Programs are expressed as a
> sequence of iterations, in each of which a vertex can receive messages sent
> in the previous iteration, send mes- sages to other vertices, and modify its
> own state and that of its outgoing edges or mutate graph topology. This
> vertex-centric approach is flexible enough to express a broad set of
> algorithms. The model has been designed for efficient, scalable and
> fault-tolerant implementation on clusters of thousands of commodity
> computers, and its implied synchronicity makes reasoning about programs
> easier. Distribution-related details are hidden behind an abstract API.
> The result is a framework for processing large graphs that is expressive
> and easy to program.

It's API was inspired by Apache Giraph.

## Configuration options

### Common configuration options (for master and worker)

* `workers` - list of all [URIs](https://tarantool.org/doc/book/configuration/index.html#uri)
	with worker (`table` of `string`s). Neccesary to define

	Example:

	``` lua
	local workers = {
		'myhost1:myport1',
		'myhost1:myport2',
		'myhost2:myport3',
	}
	-- or, for simple generation:
	local fun = require('fun')
	local function generate_worker_uris(cnt)
		cnt = cnt or 8
		return fun.range(cnt):map(function(k)
			return 'myhost1:' .. tostring(3301 + k)
		end):chain(fun.range(cnt):map(function(k)
			return 'myhost2:' .. tostring(3301 + k)
		end)):chain(fun.range(cnt):map(function(k)
			return 'myhost3:' .. tostring(3301 + k)
		end)):totable()
	end
	local workers = generate_worker_uris(cnt)
	```

* `pool_size` - Size of outgoing pool messages (`number`).
* `obtain_name` - Callback for obtaining name of vertex from data.
	(`function (value) -> string`). Necessary to define

	Example:
	``` lua
	local function obtain_name(value)
		return value.name
	end
	-- or
	local function obtain_name(value)
		return ('%s:%s'):format(value.part1, value.part2)
	end
	```

### Preloading configuration options

* `worker_preload` - function to preload data to nodes from all workers.
	excludes `master_preload`

	(`function (worker, opts) -> (function(self, idx, cnt) -> nil)`)

* `master_preload` - function to preload data to nodes from master only
	excludes `worker_preload`

	(`function (master, opts) -> (function(self) -> nil)`)

* `preload_args` - arguments, that must be passed to loader

Let's start with examples of worker/master loader creation:

``` lua
local ploader = require('pregel.loader')

local function worker_loader(worker, opts)
	local function loader(self, worker_idx, workers_count)
	-- loading process
	end
	return ploader.new(worker, loader)
end

local function master_loader(master, opts)
	local function loader(self)
	-- loading process
	end
	return ploader.new(master, loader)
end
```

And then you'll provide `worker_loader`/`master_loader` as parameters in the
options table.

#### API for loader

* `loader:store_vertex(vertex)` - arbitrary vertex object
* `loader:store_edge(src, dest, value)` - store direct edge, that connects
	vertices wtih names `dest` and `value`
* `loader:store_edges_batch(edge_list)` - store a number of edges
* `loader:store_vertex_edges(vertex, edge_list)` - combination of `store_vertex`
	and `store_edges_batch`
* `loader:flush()` - send all cached vertex

### Worker configuration options

* `worker_context` - set worker context (common object for all vertices, that
	located on current instance of worker and can be accessed/modified by vertex).
* `master` - [URI](https://tarantool.org/doc/book/configuration/index.html#uri)
	of master node
* `combiner` - combine messages, that needed to be sent to vertex
* `compute` - compute function

## Developing

Simple example is located in `example` folder. It finds maximum value of
vertices by communication:
* first step, everyone sends message with their values to their neighbor
* if value, that vertice receive, then it, again, informs all neighbours that
	it got value greater, than it've got before.
* everything ends, when no message sent and every vertice is halted (everyone
	got max before)

For more usages of Pregel data model you can read [paper](http://kowshik.github.io/JPregel/pregel_paper.pdf)
or read on it's [site](http://kowshik.github.io/JPregel/).

For example:
* Shortest Path:
	- http://kowshik.github.io/JPregel/developers.html#shortestpaths
	- https://cwiki.apache.org/confluence/display/GIRAPH/Shortest+Paths+Example
* PageRank Algorithm
	- http://kowshik.github.io/JPregel/developers.html#pagerank
	- http://giraph.apache.org/pagerank.html
* e.t.c.

In future it's planned to write down more examples/algorithms.

Also, it's preferably to use tarantool-pregel in conjuction with [Torch](http://torch.ch/).

> Torch is a scientific computing framework with wide support for machine
> learning algorithms that puts GPUs first. It is easy to use and efficient,
> thanks to an easy and fast scripting language, LuaJIT.

But you shouldn't use parallelization (as it'll break Tarantool evloop) or GPU
(since it doesn't integrated with our Fibers and it'll stop Tarantool)

### Master node

Master node is something that orchestrate everything. It told workers to do
something, it will have access to all results in the end, that workers will
decide to give it.

* `master:wait_up()` - wait while all workers are up and running.
* `master:start()` - start task (compute everything)
* `master:preload()` - preload all data from master
* `master:preload_on_workers()` - preload all data from workers
* `master:add_aggregator(name, options)` - add agreggator.

	Possible options are:
	* `default` - default value for aggregator. Can be anything.
	* `merge` - Add new value to aggregator. Must be commutative and associative.

	  Example:
		```
		local function merge(old, new)
			return old + new
		end
		```

* `master:save_snapshot()` - tell workers to save snapshot.

Typical master initialization is like that:

```
local pmaster = require('pregel.master')
local master = pmaster.new('test', config)
master:add_aggregator('custom', <options>)
master:wait_up()
master:preload_on_workers()
<...> -- for example, you can add vertices by hand, if you need it to.
master:save_snapshot()
```

### Worker node

* `master:add_aggregator(name, options)` - add agreggator.

	Possible options are:
	* `default` - default value for aggregator. Can be anything.
	* `merge` - Add new value to aggregator. Must be commutative and associative.

	  Example:
		```
		local function merge(old, new)
			return old + new
		end
		```

### Vertex

Vertex is the least and main part of this process. Compute function is applied
to each vertex on each worker node.

#### API

**Base API**:

* `vertex:vote_halt([is_halted = true])` - set vertex status to be halted
* `vertex:pairs_edges()` - iterate through all edges.

	Example:
	```
	for _, neighbour, value in vertex:pairs_edges() do
		-- process vertex
	end
	```

* `vertex:get_value()` - get value of vertex
* `vertex:set_value(value)` - set value of vertex
* `vertex:get_name()` - get name of vertex
* `vertex:get_superstep()` - get superstep number (1 to ...)

**Messaging API**

> Vertices communicate directly with one another by sending messages, each of
> which consists of a message value and the name of the destination vertex.
> A vertex can send any number of messages in a superstep. All messages sent to
> vertex V in superstep S are available, via an iterator, when V’s `Compute()`
> method is called in superstep S + 1. There is no guaranteed order of messages
> in the iterator, but it is guaranteed that messages will be delivered and that
> they will not be duplicated. A common usage pattern is for a vertex V to
> iterate over its outgoing edges, sending a message to the destination vertex
> of each edge.
>
> However, `dest_vertex` need not be a neighbor of V. A vertex could learn the
> identifier of a non-neighbor from a message received earlier, or vertex
> identifiers could be known implicitly. For example, the graph could be a
> clique, with well-known vertex identifiers V1 through Vn, in which case there
> may be no need to even keep explicit edges in the graph. When the destination
> vertex of any message does not exist, we execute user-defined handlers. A
> handler could, for example, create the missing vertex or remove the dangling
> edge from its source vertex.

* `vertex:pairs_messages()` - iterate through all incoming messages.

	Example:
	```
	for _, message in vertex:pairs_messages() do
		-- process all incoming messages
	end
	```
* `vertex:send_message(receiver_id, msg)` - send message to vertex with ID
	`receiver_id`

**Aggregation API**

> Pregel aggregators are a mechanism for global communication, monitoring, and
> data. Each vertex can provide a value to an aggregator in superstep S, the
> system combines those values using a reduction operator, and the resulting
> value is made available to all vertices in superstep S + 1.

* `vertex:get_aggragation(name)` - get value from aggregator
* `vertex:set_aggregation(name, value)` - set aggregator value

**Topology mutation part**

> Some graph algorithms need to change the graph’s topology. A clustering
> algorithm, for example, might replace each cluster with a single vertex,
> and a minimum spanning tree algorithm might remove all but the tree edges.
> Just as a user’s Compute() function can send messages, it can also issue
> requests to add or remove vertices or edges.

* `vertex:add_vertex(value)` - add vertex
* `vertex:add_edge([src = vertex:get_name(), ]dest, value)` - add edge
* `vertex:delete_vertex([src][, vertices = true])` - delete vertex
* `vertex:delete_edge([src = vertex:get_id(), ]dest)` - delete edge.

If you'll change properties (add/delete vertex/edge) of currently running vertex,
then it'll be applied immediatly after compute.
