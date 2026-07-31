<h3 align="center" style="margin:0px">
	<img width="200" src="https://kinetica-web-assets.s3.us-east-1.amazonaws.com/assets/kinetica_logo_gray.svg" alt="Kinetica Logo"/>
</h3>
<h5 align="center" style="margin:0px">
	<a href="https://www.kinetica.com/">Website</a>
	|
	<a href="https://docs.kinetica.com/">Docs</a>
	|
	<a href="https://docs.kinetica.com/content/connectors/beam_guide">Beam Docs</a>
	|
	<a href="https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg">Community Slack</a>   
</h5>


## Contents

* [What is the Project about?](#what-is-the-project-about)
* [Build and run the API and example](#build-and-run-the-api-and-example)
* [Build and test with the Docker environment](#build-and-test-with-the-docker-environment)
* [Support](#support)
* [Contact Us](#contact-us)


## What is the Project about? ##

This project contains a connector that allows Kinetica to be used in Apache Beam
data flows.

Apache Beam is a system for describing and running bespoke data processing
pipelines. What makes it interesting is:

1.	Portability

	You can run your Beam pipeline on a variety of backend processing stacks
	without changing the pipeline code, just by configuration.  Out of the box,
	Beam provides adapters (called "runners") to run Beam pipelines on Spark,
	Flink, Apex, Google Cloud Dataflow, Stamza and Java (the "Direct" Runner).

2.	Unified Language

	Beam pipelines are described in a programming language using model
	constructs provided by Beam. The language is powerful and based on modern
	ideas from functional programming (lambdas) etc.  What makes it special,
	however, is that it provides a single unified model to describe both batch
	and stream processing in a single, coherent way. Essentially everything is
	treated as a stream, and Batch is treated as a special case where the
	time-window is very long.

3.	Language Independence

	Beam is designed to be language independent, so that you can (in principle)
	write your Beam pipelines in a language of your choice. Right now it
	provides language bindings for Java, Python, Go and Scala. However, in
	practice, this is an evolving story and right now most of the Beam ecosystem
	is Java specific. This Kinetica Beam connector supports Java Beam pipelines
	(requires Java 11+).

4.	Extensibility

	Beam is designed to be extensible in a number of ways:

	*	Beam allows you to connect to a variety of data stores that can then be
	    used as data Sources and Sinks in your Beam pipeline. Out of the box,
	    Beam allows you to connect to a wide variety of data stores including:
	    the file system, Cassandra, MongoDB, Redis, Hadoop, Amazon file storage,
	    Elastic, Hbase, JDBC etc. However, it is possible (with a bit of work)
	    to add new data sources/sinks that can then be used in Beam pipelines.
	    This is the approach taken by the Kinetica connector
	*	It is possible to add new Runners, so that beam pipelines can run on
	    additional distributed processing stacks. It would be possible, in
	    principle, to build a Kinetica Runner so that Kinetica is used not only
	    as a source/sink of data but also to run the Beam pipelines themselves.
	    This is not the approach taken here (a future project perhaps).

You can find more info on Apache Beam here: https://beam.apache.org/

So as a Beam developer, it works like this:

*	You write your Beam pipeline as normal using entirely standard Beam
	programming constructs
*	You build your project against the API JAR file provided by this project.
	This allows you to write Beam pipelines that access Kinetica as a data
	source/sink. An example is provided in this project of how to do this.
*	You build and run your pipeline on your favorite distributed processing
	stack: Spark, Flink etc. At runtime, your pipeline accesses Kinetica as
	required through standard Kinetica API calls to read and write data.


## Build and run the API and example ##

Compile the API starting from the `kinetica-connector-beam` directory.

	cd api
	mvn clean install

Compile the example.

	cd ../example
	mvn clean package

Run the example.

	java -jar target/apache-beam-kineticaio-example-7.2.0.0.jar \
		--kineticaURL=<url> \
		--kineticaUsername=<username> \
		--kineticaPassword=<password>

The example will create a table and use Beam to insert 1000 scientist names into
it.  It will then use Beam to read the names from the table and output them to
the console.  The output should look similar to the following.

	2022-09-02 12:27:34.039 [main] INFO  c.k.beam.example.ExampleBeamPipelinemain:34 - Example starting...
	2022-09-02 12:27:34.468 [main] INFO  c.k.beam.example.ExampleBeamPipelinemain:38 - Current Settings:
	  appName: ExampleBeamPipeline
	  kineticaPassword: admin
	  kineticaURL: http://127.0.0.1:9191
	  kineticaUsername: admin
	  optionsId: 0
	
	2022-09-02 12:27:34.627 [main] INFO  c.k.beam.example.KineticaTestDataSetgetGPUdb:52 - *** Connecting to GPUDB at :http://127.0.0.1:9191 as admin
	2022-09-02 12:27:37.222 [direct-runner-worker] INFO  c.k.beam.example.ExampleBeamPipelineprocessElement:114 - Name: Lovelace, id: 200
	...
	2022-09-02 12:27:37.314 [direct-runner-worker] INFO  c.k.beam.example.ExampleBeamPipelineprocessElement:114 - Name: Maxwell, id: 949
	2022-09-02 12:27:37.329 [main] INFO  c.k.beam.example.ExampleBeamPipelinemain:44 - Example complete!


## Build and test with the Docker environment ##

A reference environment is provided in `docker/docker-compose.yml`. It brings up
a Kinetica 7.2 database, a Java 11 Maven builder, and a Spark 3.1 standalone
cluster with an edge node to submit jobs from. Everything is pulled from public
registries: there are no artifacts to download by hand and no license key is
required.

You need Docker with the Compose plugin (check with `docker compose version`)
and roughly 8 GB of free disk for the images.

All commands below are run from the `docker` directory:

	cd docker

### Start the database ###

	docker compose up -d kinetica

The first run pulls about 2 GB. The database takes about two minutes to come up
from an empty persist volume, so wait for the healthcheck to report `healthy`
before connecting:

	docker compose ps

	NAME       IMAGE                                              STATUS
	kinetica   kinetica/kinetica-cpu:7.2.3.18.20260707203019.ga   Up 2 minutes (healthy)

GAdmin is then at <http://localhost:8080> and the default credentials are
`admin` / `admin`.

If a port is already taken on your host — for example because you are already
running Kinetica — override it rather than editing the compose file:

	KINETICA_HTTP_PORT=19191 KINETICA_GADMIN_PORT=18080 docker compose up -d kinetica

The same applies to `KINETICA_WORKBENCH_PORT`, `SPARK_MASTER_UI_PORT`,
`SPARK_MASTER_PORT` and `SPARK_WORKER_UI_PORT`.

### Build the connector and the example ###

	docker compose up -d builder
	docker compose exec builder mvn -f api/pom.xml clean install
	docker compose exec builder mvn -f example/pom.xml clean package

The `api` project must be installed before `example` will build: there is no
parent POM tying them together, so `example` resolves the connector from the
local Maven repository. That repository is a named volume shared by the builder,
so it survives container restarts.

### Run the example on the Direct Runner ###

	docker compose exec builder \
	    java -jar example/target/apache-beam-kineticaio-example-7.2.0.0-shaded.jar \
	        --kineticaURL=http://kinetica:9191 \
	        --kineticaUsername=admin \
	        --kineticaPassword=admin \
	        --kineticaTable=scientists \
	        --runner=DirectRunner

The example creates the `scientists` table, writes 1000 rows to it with Beam,
reads them back and logs them:

	2026-07-30 19:58:46.269 [direct-runner-worker] INFO  - Name: Newton, id: 156
	...
	2026-07-30 19:58:46.288 [main] INFO  - Example complete!

Inside the compose network the database is reachable as `kinetica:9191`; from
your host it is `http://localhost:9191` (or whatever `KINETICA_HTTP_PORT` is set
to).

### Run the API integration tests ###

	docker compose exec builder mvn -f api/pom.xml verify -PrunTests

Tests are skipped unless the `runTests` profile is active. Connection settings
come from the `argLine` property in `api/pom.xml`, which points at the compose
service by default. To point the tests somewhere else, override it on the
command line instead of editing the POM:

	docker compose exec builder mvn -f api/pom.xml verify -PrunTests \
	    -DargLine="-DbeamTestPipelineOptions=[\"--kineticaURL=http://kinetica:9191\", \
	    \"--kineticaUsername=admin\", \"--kineticaPassword=admin\", \
	    \"--kineticaTable=scientists\", \"--runner=DirectRunner\"]"

**These tests are destructive.** They clear and recreate the configured table
(`scientists` by default) before running. Do not point them at a database whose
contents you care about.

### Run the example on Spark ###

Start the cluster and the edge node:

	docker compose up -d spark-master spark-worker edge-node

Rebuild the example against the Spark runner. The `spark-runner` profile is not
active by default, and selecting it deactivates `direct-runner`:

	docker compose exec builder mvn -f example/pom.xml -Pspark-runner clean package

Then submit from the edge node:

	docker compose exec edge-node /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --class com.kinetica.beam.example.ExampleBeamPipeline \
	    --conf spark.driver.host=edge-node \
	    /workspace/example/target/apache-beam-kineticaio-example-7.2.0.0-shaded.jar \
	        --kineticaURL=http://kinetica:9191 \
	        --kineticaUsername=admin \
	        --kineticaPassword=admin \
	        --kineticaTable=scientists \
	        --runner=SparkRunner

	26/07/30 20:29:30 INFO SparkRunner: Batch pipeline execution complete.
	26/07/30 20:29:30 INFO ExampleBeamPipeline: Example complete!

`spark.driver.host=edge-node` is required: the driver runs on the edge node in
client mode, and the executors need a resolvable address to connect back to. The
Spark master UI is at <http://localhost:8082> and the worker UI at
<http://localhost:8083>.

Note that the Spark version is pinned to 3.1.x on purpose. Beam 2.41.0's
`beam-runners-spark-3` is compiled against Spark 3.1.2 and declares the Spark
artifacts `provided`, so the cluster has to supply a compatible runtime; a newer
Spark is not a drop-in substitute.

### Shut down ###

	docker compose down

Add `-v` to also drop the database persist volume and the Maven cache:

	docker compose down -v

### Notes ###

*	**Java 11 is required.** The POMs require `gpudb-api` 7.2.3.22 or newer, and
	the Kinetica Java API has been compiled for Java 11 since 7.2.3.7 (7.2.3.6
	was the last Java 8 build). Building on JDK 8 fails with `class file has
	wrong version 55.0, should be 52.0`.
*	**The API and database versions move independently.** The compose file pins
	the database to 7.2.3.18, the newest published `kinetica/kinetica-cpu` image;
	the Java API is separately at 7.2.3.22. They do not need to match, and this
	combination is what the instructions above were verified against.
*	**The database does not start by default.** The Kinetica image starts only
	the host manager unless `GPUDB_START_ALL=1` is set, which the compose file
	does. Without it the container runs but nothing listens on 9191.
*	**Jackson is pinned to 2.22.1 — do not lower it.** Both POMs import
	`jackson-bom` at that version, matching the pin `gpudb-api` applies
	internally as of 7.2.3.22. A project's own dependency management outranks a
	dependency's, so a lower value here would silently override that pin and
	reintroduce the advisories it clears; keep the two in step. The block stays
	even though the API now pins Jackson itself, because without it the resolved
	version depends on the order of the declared dependencies: `gpudb-api` and
	`beam-sdks-java-core` both contribute Jackson at the same depth and Beam asks
	for 2.13.0, so reordering them would quietly drop Jackson to 2.13.0.
*	**Downgrades fail the build.** Both POMs run the enforcer's
	`requireUpperBoundDeps` rule, so if a managed dependency resolves below a
	version something else in the graph requests, the build stops instead of
	silently shipping the downgrade. The rule is scoped to the artifacts these
	POMs manage — applying it graph-wide would mean arbitrating Beam's own
	transitive conflicts. If you add a version pin, expect to add the artifact to
	the `includes` list too. List artifacts explicitly: a `groupId:*` wildcard
	matches nothing here and silently disables the check.
*	**Plugin versions are pinned.** The example pins `maven-shade-plugin` and
	`maven-jar-plugin` explicitly. Unversioned, they resolve from Maven's default
	lifecycle bindings and vary by Maven version — with Jackson 2.22.x, Maven
	3.8.x selects a shade plugin whose ASM cannot read the jar's Java 21
	`META-INF/versions/21` entries and fails with "Unsupported class file major
	version 65".
*	**Running from the host needs care.** The Java client asks the server for its
	other hosts, and a container reports its compose-internal address. Running
	against `http://localhost:9191` from outside Docker can therefore fail with a
	connect timeout to a `172.x` address. Run inside the compose network, as the
	commands above do.
*	**Connection failures kill the JVM.** On a `GPUdbException` the connector
	logs and calls `System.exit(99)`. Under Maven this surfaces as surefire's
	"The forked VM terminated without properly saying goodbye" — the usual cause
	is an unreachable database or bad credentials, not a build problem.


## Support

For bugs, please submit an
[issue on Github](https://github.com/kineticadb/kinetica-connector-beam/issues).

For support, you can post on
[stackoverflow](https://stackoverflow.com/questions/tagged/kinetica) under the
``kinetica`` tag or
[Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg).


## Contact Us

* Ask a question on Slack:
  [Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg)
* Follow on GitHub:
  [Follow @kineticadb](https://github.com/kineticadb) 
* Email us:  <support@kinetica.com>
* Visit:  <https://www.kinetica.com/contact/>
