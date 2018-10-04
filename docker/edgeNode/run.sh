#!/bin/bash
docker run --name beam-edge-node -it --rm --network beamnet \
-v C:/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/example/:/usr/local/beam/example-project \
-v beamio_beam-edge-node-mvn-cache:/root/.m2 \
-v /c/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/docker/edgeNode/resources:/usr/local/beam/resources \
apwynne/beam-spark-driver bash

