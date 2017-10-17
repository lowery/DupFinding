#/bin/sh -f
mvn clean
mvn install -Dmaven.test.skip=true


java -jar target/FP-DuplicateFinding-0.0.1-SNAPSHOT-jar-with-dependencies.jar Run -configFileName /nevpDup.properties -vectorize
java -jar target/FP-DuplicateFinding-0.0.1-SNAPSHOT-jar-with-dependencies.jar Run -configFileName /nevpDup.properties -cluster
java -jar target/FP-DuplicateFinding-0.0.1-SNAPSHOT-jar-with-dependencies.jar Run -configFileName /nevpDup.properties -dumpClusters
java -jar target/FP-DuplicateFinding-0.0.1-SNAPSHOT-jar-with-dependencies.jar Run -configFileName /nevpDup.properties -interpretClusters
