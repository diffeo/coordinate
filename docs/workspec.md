Coordinate Work Specs
=====================

Work in Coordinate is divided among _work specs_.  A work spec
describes a family of related jobs, in typical use that call the same
Python function.  Each job is a _work unit_, which is associated with
a single work spec, and has a name (a _key_) and _data_, almost always
a dictionary.  There can be any number of work specs and any number of
work units, but Coordinate's overall design assumes at most dozens of
work specs with millions (or more) of work units each.

A work spec is always represented as a dictionary.  The system
requires two keys to always be present, `name` and `min_gb`.  In
practice more keys are present.  The work spec dictionary may contain
any keys that are recognized by the system or the run function;
unrecognized keys are ignored.  Where specific behaviors of the system
are documented, these are consistent with Coordinate 0.1.5.

Core Work Spec Keys
-------------------

These keys are used by the core system in some form.

`name` --- Gives the name of the work spec.  Required.

`min_gb` --- Gives the minimum physical memory requirement to run the
job.  Required, but not actually considered by the job scheduler.

`desc` --- Gives a longer description of the work spec.  Useful for
user documentation, but not used by the system.

Python Worker
-------------

These keys are used by the Python worker, as embodied in the
`coordinate run_one` and `coordinate_worker` commands.  In this worker
each work unit translates to a call of a function named in the work
spec.

`module` --- Python module containing the function.  Required.

`run_function` --- Name of the object in `module` to call.  Required.

`config` --- A complete [yakonfig](https://github.com/diffeo/yakonfig)
system configuration that will be used by this work spec.  Not used by
Coordinate proper beyond specific handling in `coordinate flow`, and
optional in this case.

Concurrency
-----------

Coordinate will usually let any number of workers work on any number
of work units for a given work spec, though only one worker will own
any given work unit.  There are several controls over this.

`max_running` --- Gives the maximum number of concurrent "pending"
work units for this work spec, across the entire system.  The
scheduler will return nothing or a different work spec if this many
units are already pending.  Defaults to unlimited.

`max_getwork` --- Gives the maximum number of work units that will be
returned to a worker in one shot.  Defaults to unlimited.  Note that
the worker must explicitly request multiple jobs, and
`coordinate_worker` will only request one at a time unless its
`worker_job_fetch` configuration parameter is increased.

Continuous and Scheduled Jobs
-----------------------------

Coordinate has limited ability to run periodic jobs, and to generate
synthetic jobs for background tasks.

`continuous` --- If true, the scheduler may create new work units for
this work spec.  There must be no other work units for this work spec.
The work unit key is arbitrarily chosen but probably unique (the
current implementation uses the current time) and the work unit data
is an empty dictionary.  Defaults to false.

`interval` --- Gives a minimum time, in seconds, between creating
continuous work units.  If zero or unspecified, the system will create
new continuous work units immediately.  Defaults to unspecified.

Work Spec Chaining
------------------

A work spec can give the name of a work spec that should run
immediately after it.  If the work spec contains a `then` key, and a
successfully completed work unit for that work spec has an `output`
key in its data dictionary, then new work units will be created.
`output` may be:

* A dictionary mapping work unit key to data
* A list of strings; these work units are created with empty data
  dictionaries
* A list of 2-tuples where each tuple is (key, data)
* A list of 3-tuples (key, data, options); options is a dictionary
  where the only recognized key is `priority`

Relevant work spec keys include:

`then` --- Name of the following work spec

`then_preempts` --- If true (the default) then work units of the
`then` spec take unconditional precedence over this one.

Scheduling
----------

The Coordinate server contains a work spec scheduler with some basic
controls.  The core of the scheduler involves analyzing the `then`
work spec chains to find preempting and continuous specs and choosing
between those.  Each work spec has a weight, and with enough
concurrent workers and enough available work units, subject to the
other constraints, work specs will receive a number of jobs
proportional to their weight.

The scheduler filters work specs based on their `min_gb`, omits work
specs that have no work units and are not `continuous`, and does not
consider work specs that have already reached `max_running`.  If a
work spec has a `then` work spec, `then_preempts` is unspecified or
true, and the later work spec has work, then the earlier one will not
be considered unless the `then` chain forms a loop.

Normal work specs and chains, chained work specs forming a loop, and
continuous work specs are handled separately.  Without additional
configuration, weights of work specs in a loop are effectively halved,
and continuous work specs run with 1/20 the weight of normal ones.

_Nota bene:_ This scheduling system has its complexity in the wrong
places.  The preëmptive nature of `then` doesn't actually apply to
most use cases, complicated work spec chain topologies are extremely
rare, and a much simpler weight/priority scheme would better address
the common case.  The scheduling system may change in the future.

`weight` --- Relative weight of this work spec.  Default weight is 20.

Flows
-----

`coordinate flow flow.yaml` allows for multiple work specs to be
loaded from a file in a single operation.  The YAML file is a
dictionary with a top-level key, `flows`.  Each item in this
dictionary is a work spec, where the key provides the work spec `name`
and the value is a dictionary with the remainder of the work spec
data.

A `config` key is treated specially.  If there is no `config` key, the
[yakonfig](https://github.com/diffeo/yakonfig) configuration in effect
for the `coordinate` program is injected into the work spec `config`.
If `config` is included in the flow file, it is overlaid over the
running configuration.  This allows `config` to only contain the
configuration needed to execute a work spec, and not system-level
parameters such as database locations.

The flow file may have a second top-level key, `yakonfig_modules`,
which is a list of Python module names (only) passed as configuration
module parameters to yakonfig.

_Nota bene:_ the "flow" name also follows from a vision of many `then`
declarations.  `coordinate load` provides similar functionality but
requires one YAML (or JSON) file per work spec, and does not support
the configuration merging.
