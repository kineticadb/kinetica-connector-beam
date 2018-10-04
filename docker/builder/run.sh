#!/bin/bash
docker run --name beam-builder -it --rm --network beamnet \
-v C:/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/api/:/usr/local/beam/api \
-v beamio_beam-builder-mvn-cache:/root/.m2 \
-v /c/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/docker/builder/resources:/usr/local/beam/resources \
apwynne/beam-builder bash

