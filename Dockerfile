FROM openjdk:8
RUN  \
  export DEBIAN_FRONTEND=noninteractive && \
  sed -i 's/# \(.*multiverse$\)/\1/g' /etc/apt/sources.list && \
  apt-get update && \
  apt-get -y upgrade && \
  apt-get install -y vim wget curl git maven
RUN git clone https://github.com/vikas-getconnect/loader.git
WORKDIR /loader
RUN git pull
RUN mvn install:install-file -Dfile=lib/couchbase-transactions-1.0.0-alpha.4.jar -DgroupId=com.couchbase -DartifactId=couchbase-transactions -Dversion=1.0.0-alpha.4 -Dpackaging=jar
RUN mvn clean package assembly:single
ENTRYPOINT ["java","-jar","target/loader-1.0-SNAPSHOT.jar"]