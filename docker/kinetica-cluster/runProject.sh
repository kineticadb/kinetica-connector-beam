# docker run -d --privileged --rm --name kinetica-6.2 -p 8084:8080 -p 9192:9193 -p 8088:8088 -v kinetica-6.2-persist:/opt/gpudb/persist -v /c/Users/apwyn/Documents/DockerContainers/kinetica-6.2/mount/gpudb-logs/:/opt/gpudb/core/logs/ apwynne/kinetica-6.2

docker run  \
  --name kinetica-6.2-head \
  -d --privileged --rm \
  --network beamnet --ip 172.19.0.10 \
  -p 8080:8080 -p 8088:8088 -p 8181:8181 -p 9001:9001 -p 9002:9002 -p 9191:9191 -p 9292:9292 \
  -v kinetica-6.2-head-persist:/opt/gpudb/persist \
  -v /c/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/docker/kinetica-cluster/mount/kinetica-head/gpudb-logs/:/opt/gpudb/core/logs/ \
  apwynne/kinetica-6.2-node

docker run \
  --name kinetica-6.2-worker \
   -d --privileged --rm \
   --network beamnet --ip 172.19.0.11 \
   -v kinetica-6.2-worker-persist:/opt/gpudb/persist \
   -v /c/Users/apwyn/Documents/eclipse-workspace/apache-beam-kineticaio/docker/kinetica-cluster/mount/kinetica-worker/gpudb-logs/:/opt/gpudb/core/logs/ \
   apwynne/kinetica-6.2-node 


# docker exec -it kinetica-6.2-head  /etc/init.d/gpudb_host_manager start


