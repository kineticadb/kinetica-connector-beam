FROM centos

WORKDIR /usr/local/beam

COPY resources/jdk-8u162-linux-x64.rpm downloads/
ADD resources/apache-maven-3.5.3-bin.tar.gz downloads/
ADD resources/spark-2.3.1-bin-hadoop2.7.tgz /opt

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/apache-maven-3.5.3/bin
ENV JAVA_HOME /usr/java/jdk1.8.0_162/

RUN yum localinstall -y downloads/jdk-8u162-linux-x64.rpm && rm downloads/jdk-8u162-linux-x64.rpm && \
mv downloads/apache-maven-3.5.3 /opt

EXPOSE 8081 4040

CMD ["/bin/bash", "-c", \
"mvn install:install-file -Dfile='resources/apache-beam-kineticaio-1.0.jar' \
-DgroupId='com.kinetica' -DartifactId='apache-beam-kineticaio' -Dversion='1.0' -Dpackaging=jar"]

