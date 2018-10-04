FROM centos:centos7

WORKDIR /usr/local/kinetica/

COPY resources/gpudb-intel-license-6.2.0.15.20180811184611.ga-0.el6.x86_64.rpm downloads/
# COPY resources/gpudb-intel-license-6.2.0.3.20180419215402.ga-0.el6.x86_64.rpm downloads/

RUN yum localinstall -y --nogpgcheck downloads/gpudb-intel-license-6.2.0.15.20180811184611.ga-0.el6.x86_64.rpm && rm downloads/gpudb-intel-license-6.2.0.15.20180811184611.ga-0.el6.x86_64.rpm

COPY resources/gpudb.conf /opt/gpudb/core/etc/gpudb.conf

EXPOSE 2003 4000-4019 5552 5553 6555-6575 8080 8082 8088 8181 9001 9002 9191-9211 9292 9300

CMD ["/bin/sh","-c","chmod 777 /opt/gpudb/core/etc/gpudb.conf && ldconfig && /etc/init.d/gpudb_host_manager start && while /opt/gpudb/core/bin/gpudb host-manager-status >/dev/null 2>&1 ; do sleep 20; done"]
