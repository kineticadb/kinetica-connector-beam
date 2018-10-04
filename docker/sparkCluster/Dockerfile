FROM centos

COPY resources/jdk-8u162-linux-x64.rpm /
RUN yum localinstall -y /jdk-8u162-linux-x64.rpm  && rm /jdk-8u162-linux-x64.rpm
ADD resources/spark-2.3.1-bin-hadoop2.7.tgz /opt

EXPOSE 8080 7077 8083
CMD ["/bin/bash", "-c", \
"/opt/spark-2.3.1-bin-hadoop2.7/sbin/start-master.sh && \
/opt/spark-2.3.1-bin-hadoop2.7/sbin/start-slave.sh --host `hostname -i` --port 8083 spark://`hostname -i`:7077 && \
while ps -ef | grep spark > /dev/null 2>&1 ; do sleep 20; done"]