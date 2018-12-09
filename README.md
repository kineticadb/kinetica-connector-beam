## What is the Project about? ##

This project contains a connector that allows Kinetica to be used in Apache Beam data flows.

Apache Beam is a system for describing and running bespoke data processing pipelines. What makes it interesting is:

1.	Portability

	You can run your Beam pipeline on a variety of backend processing stacks without changing the pipeline code, just by configuration.  Out of the box, Beam provides adapters (called "runners") to run Beam pipelines on Spark, Flink, Apex, Google Cloud Dataflow, Stamza and Java (the "Direct" Runner)

2.	Unified Language

	Beam pipelines are described in a programming language using model constructs provided by Beam. The language is powerful and based on modern ideas from functional programming (lambdas) etc..
	What makes it special, however, is that it provides a single unified model to describe both batch and stream processing in a single, coherent way. Essentially everything is treated as a stream, and Batch is treated as a special case where the time-window is very long.

3.	Language Independence

	Beam is designed to be language independent, so that you can (in principle) write your Beam pipelines in a language of your choice. Right now it provides language bindings for Java, Python, Go and Scala. However in practice this is an evolving story and right now most of the Beam ecosystem is Java specific. This Kinetica Beam connector supports Java Beam pipelines (requires Java 8+).

4.	Extensibility

	Beam is designed to be extensible in a number of ways:

	*	Beam allows you to connect to a variety of data stores that can then be used as data Sources and Sinks in your Beam pipeline. Out of the box, Beam allows you to connect to a wide variety of data stores including: the file system, Cassandra, MondoDB, Redis, Hadoop, Amazon file storage, Elastic, Hbase, JDBC etc. However it is possible (with a bit of work) to add new data sources/sinks that can then be used in Beam pipelines. This is the approach taken by the Kinetica connector
	*	It is possible to add new Runners, so that beam pipelines can run on additional distributed processing stacks. It would be possible in principle to build a Kinetica Runner so that Kinetica is used not only as a source / sink of data but also to run the Beam pipelines themselves. This is not the approach taken here (a future project perhaps).

You can find more info on Apache Beam here : https://beam.apache.org/

So as a Beam developer, it works like this:

*	You write your Beam pipeline as normal using entirely standard Beam programming constructs
*	You build your project against the API JAR file provided by this project. This allows your to write Beam pipelines that access Kinetica as a data aource/sink. An example is provided in this project of how to do this.
*	You build and run your pipeline on your favourite distributed processing stack: Spark, Flink etc. At runtime your pipeline accesses Kinetica as required through standard Kinetica API calls to read and write data

## Build and test the API ##

### Set up your docker environment ###

In this example we are going to be running the following software on Docker to simulate a small site install :

*	A two-node kinetica cluster
*	A small spark cluster with one worker
*	An edge node machine running the Beam example

This is going to need a sensible amount of resource allocating to Docker Engine in order to run all this software - otherwise you may get slowness or Spark jobs refusing to start up. As a minimum, set the Docker Engine to have the following:

*	CPUs: 4
*	Memory: 14G
*	Swap: 1G

The examples here can also be reproduced on machines without Docker - all the necessary info is in the Dockerfiles and scripts but no specific instructions are given here as it is very site-specific.

If you haven't before, first create the docker network bridge

	$ docker network create beamnet

Now check the subnet that the network is using on your machine:

	$ docker network inspect beamnet | grep Subnet

The config below is set up for a subnet of 172.19.0.0/16 but your mileage may vary. Make a note of the subnet for later.

### Build and run the Kinetica cluster on docker ###

First we need to create the gpudb.conf file. The build script will copy this into the docker image, so we need to edit that file before building the docker images.

An example file is provided in /docker/kinetica-cluster/resources/gpudb.conf.template for kinetica version 7.0.0.15 - all you need to do is:
1.	copy /docker/kinetica-cluster/resources/gpudb.conf.template to /docker/kinetica-cluster/resources/gpudb.conf
2.	edit gpudb.conf and set the license_key parameter

This file has the following config :

* license_key = <configure your license key>
* head_ip_address = 172.19.0.10 # static IP of kinetica-7.0-head docker container
* enable_worker_http_servers = true # turn on multi-head ingest support
* rank0.host = 172.19.0.10 # static IP of kinetica-7.0-head docker container
* rank1.host = 172.19.0.10 # static IP of kinetica-7.0-head docker container
* rank2.host = 172.19.0.11 # static IP of kinetica-7.0-worker docker container

In the above config we are using a subnet of 172.19.0.0/16. If your subnet is different go ahead and edit these IP addresses to match your subnet

Now we need to download the Kinetica binary from http://repo.kinetica.com and copy into docker/kinetica-cluster/resources/<kinetica-rpm-filename>

Now create the docker image for the kinetica cluster nodes. The build script copies the gpudb.conf file into the docker image - if you need to change the gpudb.conf file for some reason just rebuild the image (the COPY step is near the end so should be quick)

	$ cd docker/kinetica-cluster
	$ ./build.sh

This creates a docker image called apwynne/kinetica-7.0-node. confirm as follows:

	$ docker image ls | grep kinetica-7.0-node
    apwynne/kinetica-7.0-node

Now create the directories on the host machine that the docker containers will mount, for writing Kinetica logs :

	$ mkdir -p docker/kinetica-cluster/mount/kinetica-head/gpudb-logs/
	$ mkdir -p docker/kinetica-cluster/mount/kinetica-worker/gpudb-logs/


Now edit the runProject.sh script file. Look for the argument --ip 172.19.0.10 in the line that starts the Kinetica head node and change it to the IP you selected earlier for the head node to go with your subnet. Similarly, look for the argument --ip 172.19.0.11 on the line that starts the Kinetica worker node and change that to the IP you selected earlier for your subnet.

Now we can run up the cluster

	$ ./runProject.sh

This will start 2 docker containers to form a small Kinetica cluster; the containers are called kinetica-7.0-head and kinetica-7.0-worker. These are given fixed IP addresses which correspond to the expected IP addresses in the gpudb.conf script as described above.

On the host machine pull up a browser and login at http://localhost:8080/gadmin and start the cluster.

### Create and run the beam-builder docker container ####

First download the resources from the web as follows and place them in the 'docker/builder/resources' directory:

* Java 8 JDK (jdk-8u162-linux-x64.rpm)
* Apache Maven (apache-maven-3.5.3-bin.tar.gz)

Now create and run the docker container:

	$ cd docker/builder
	$ ./build.sh
	$ ./run.sh

This creates a docker image and then runs a container called called beam-builder that will be used to build the API.

The build process includes an optional 'verify' stage that connects to a Kinetica cluster and runs some tests. The builder container is on the same docker bridge network as the Kinetica cluster so that the nodes can communicate freely for bulk insert operations.

### To build the API Jar ###

In the beam-builder, at the # prompt

	# cd /usr/local/beam/api
	# mvn clean package

This will create the API jar in target/apache-beam-kineticaio-1.0.jar. Use this in your Beam code

Hit ctrl-D to exit from the # prompt and stop the builder container

### To unit-test the API Jar ###

This step is optional. It uses the DirectRunner to run some simple read/write tests against the Kinetica cluster using Apache Beam.

Make sure you started the Kinetica cluster as described above before running this step

Run the beam-builder docker container, if it's not already running

	$ cd docker/builder
	$ ./run.sh

In the beam-builder, at the # prompt first make sure the Kinetica cluster is up

	# curl http://kinetica-7.0-head:9191

It should say "Kinetica is running!"

Now edit the api/pom.xml to give the connection details for your Kinetica server. Look for the line:

	    <argLine>-DbeamTestPipelineOptions='["--kineticaURL=http://kinetica-7.0-head:9191", "--kineticaUsername=admin", "--kineticaPassword=Kinetica1!", "--kineticaTable=scientists", "--runner=DirectRunner"]'</argLine>

beamTestPipelineOptions is a Json array. You can set the following parameters as required:

* kineticaPassword
* kineticaUsername. defaults to "admin"
* kineticaURL. defaults to "http://localhost:9191"
* kineticaTable. defaults to "scientists". This is a test table that gets created and accessed during the unit tests
* runner. defaults to DirectRunner. Don't change this.

Now in the api directory run the unit tests:

	# mvn verify

This will:

* create the API jar in target/apache-beam-kineticaio-1.0.jar, if you didn't already do it (above)
* build and run the Junit tests using the Beam Direct Runner. You should see 2 tests ran without error

If you login to GAdmin, you will see the tests created a test table called "scientists". You can safely delete this if required

Hit ctrl-D to exit from the # prompt and stop the builder container

## To debug using Eclipse and the Direct Runner example ##

Create a Debug Configuration of type JUnit with following settings:

* run all tests in the project: apache-beam-kineticaio-api

Now create a breakpoint e.g. in the test code and run the Debug Configuration

Be aware there doesn't seem to be any way to pass arguments in for kineticaPassword etc. So either inject those into variables using the debugger or just set the defaults in IOTestPipelineOptions.java

## Running The Spark Example ##

### Build and start the Spark cluster ###

First download the resources from the web as follows and place them in the 'docker/sparkCluster/resources' directory:

* Java 8 JDK (jdk-8u162-linux-x64.rpm)
* Apache Spark (spark-2.3.1-bin-hadoop2.7.tgz)

Now create and run the docker container:

	$ cd docker/sparkCluster
	$ ./build.sh
	$ ./run.sh

This builds a docker image called apwynne/spark-cluster and runs a docker container called spark-cluster

On the host machine open a browser window at http://localhost:8082/

### Build and start the Edge Node ###

First download the resources from the web as follows and place them in the 'docker/edgeNode/resources' directory:

* Apache Maven (apache-maven-3.5.3-bin.tar.gz)
* Java 8 JDK (jdk-8u162-linux-x64.rpm) # from Oracle website
* Kinetica Beam Connector (apache-beam-kineticaio-1.0.jar)  # from building the API - see above
* Apache Spark (spark-2.3.1-bin-hadoop2.7.tgz)

Build the source and docker images:

	$ cd docker/edgeNode
	$ ./build.sh

This creates a docker image for the edgeNode called apwynne/beam-spark-driver

Start the beam-edge-node container, using this image.

	$ cd docker/edgeNode
	$ ./run.sh

At the # command prompt check you can ping the other containers:

	# ping kinetica-7.0-head
	# ping kinetica-7.0-worker
	# ping spark-cluster

check Kinetica is listening as follows:

	# curl http://`getent hosts kinetica-7.0-head | cut -d ' ' -f1`:9191
	Kinetica is running!

Now we need to install the Beam connector API Jar into the maven repo on the edgeNode:

	# mvn install:install-file -Dfile='resources/apache-beam-kineticaio-1.0.jar' -DgroupId='com.kinetica' -DartifactId='apache-beam-kineticaio' -Dversion='1.0' -Dpackaging=jar


### Run the Beam job on Spark ###

At the # command prompt of the beam-edge-node container

First build the the source code with Spark dependencies

	# cd /usr/local/beam/example-project
	# mvn -Pspark-runner clean package

This builds the JAR (apache-beam-kineticaio-example-1.0-shaded.jar) and copies it into /usr/loca/beam/resources

Now run the beam job:

	# cd /usr/local/beam
	# cp example-project/target/apache-beam-kineticaio-example-1.0-shaded.jar .
	# /opt/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --master spark://`getent hosts spark-cluster | cut -d ' ' -f1`:7077 --conf spark.executor.memory=2G --class com.kinetica.beam.example.Test1 apache-beam-kineticaio-example-1.0-shaded.jar --kineticaPassword=Kinetica1! --kineticaTable=scientists --kineticaURL=http://`getent hosts kinetica-7.0-head | cut -d ' ' -f1`:9191 --kineticaUsername=admin --runner=SparkRunner

NB We copy the jar as it won't run from a docker mount, at least on Windows WSL

Hit ctrl-D to exit and stop the beam-edge-node container

Check the Spark UX at http://localhost:8082/ to confirm the job should have run successfully and make a note of the ApplicationIDs. There will be 2 ApplicationIDs for every run - one for the read test (first) and then the write test

To review the job logs:

	$ docker exec -it spark-cluster bash

and at the hash prompt in the spark-cluster container, do:

	# cd /opt/spark-2.3.1-bin-hadoop2.7/work/<ApplicationID> # check job logs from here

Also logon to http://localhost:8080 and in Kinetica Gadmin, check the table 'scientists' has been created OK and populated with data from the write test


### Run the Beam job on the Direct Runner ###

Start the beam-edge-node container:

	$ cd docker/edgeNode
	$ ./run.sh

At the # command prompt check you can ping the other containers:

	# ping kinetica-7.0-head
	# ping kinetica-7.0-worker
	# ping spark-cluster

check Kinetica is listening as follows:

	# curl http://`getent hosts kinetica-7.0-head | cut -d ' ' -f1`:9191

It should say "Kinetica is running!"

First build the source code with DirectRunner dependencies

	# cd /usr/local/beam/example-project
	# mvn -Pdirect-runner clean package

This builds the JAR (apache-beam-kineticaio-example-1.0-shaded.jar) and copies it into /usr/loca/beam/resources

Now run the beam job:

	# cd /usr/local/beam
	# cp example-project/target/apache-beam-kineticaio-example-1.0-shaded.jar .
	# java -cp apache-beam-kineticaio-example-1.0-shaded.jar \
	com.kinetica.beam.example.Test1 \
	--kineticaURL="http://`getent hosts kinetica-7.0-head | cut -d ' ' -f1`:9191" \
	--kineticaUsername=admin \
	--kineticaPassword=Kinetica1! \
	--kineticaTable=scientists \
	--runner=DirectRunner

Hit ctrl-D to exit and stop the beam-edge-node container
