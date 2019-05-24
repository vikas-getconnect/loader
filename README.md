# loader
## Create Java jar to run
- mvn install:install-file -Dfile=lib/couchbase-transactions-1.0.0-alpha.4.jar -DgroupId=com.couchbase -DartifactId=couchbase-transactions -Dversion=1.0.0-alpha.4 -Dpackaging=jar
- mvn clean package assembly:single

## Run as 
java -jar target/loader-1.0-SNAPSHOT.jar 
