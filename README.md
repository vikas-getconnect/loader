# loader
Loader is a data to run transactions on couchbase cluster.

## Create Java jar to run
- mvn install:install-file -Dfile=lib/couchbase-transactions-1.0.0-alpha.4.jar -DgroupId=com.couchbase -DartifactId=couchbase-transactions -Dversion=1.0.0-alpha.4 -Dpackaging=jar
- mvn clean package assembly:single

## Run as 
java -jar target/loader-1.0-SNAPSHOT.jar 

## Options
```
Missing required options: h, c
usage: loader [-b <arg>] -c <arg> [-d <arg>] [-durability <arg>] -h <arg>
       [-o <arg>] [-password <arg>] [-t <arg>] [-u <arg>] [-user <arg>]
 -b <arg>            bucket name or default
 -c <arg>            create %
 -d <arg>            delete %
 -durability <arg>
                     [MAJORITY,MAJORITY_AND_PERSIST_ON_MASTER,PERSIST_TO_M
                     AJORITY]
 -h <arg>            host address
 -o <arg>            ops rate
 -password <arg>     password
 -t <arg>            Number of threads
 -u <arg>            update %
 -user <arg>         username

```
## Build a docker image
 ``` 
 docker build --no-cache -t sequoiatools/loader:latest . 
 or you can pull docker image sequoiatools/loader
 ```
## Running image 
``` docker run sequoiatools/loader <options> ```
 
