Building the jar:

mvn install -Dmaven.test.skip=true

Running the applications:

1. cd to the DuplicateFinding project root
2. Rebuild the jar if any changes have been made, especially 
in the Configuration properties files. (See below for their location)
3. Run the desired application with:

java -jar target/FP-DuplicateFinding-0.0.1-SNAPSHOT-jar-with-dependencies.jar Run -configFileName <filename> -<task> where <filename> is the configuration properties file name and <task> is one of the strings "vectorize", "cluster", "dumpClusters", "interpretClusters" (omitting quote marks).

Normally, the tasks should initially be run once successfully in order 
because each depends on the output of the previous. Occasionally there 
is some reason to rerun later tasks without going back all the way to 
"vectorize".  This may happen if some directory required by a later task 
does not exist even though the earlier tasks have succeeded.  This is a bug.
The failing task should decline to run if its resources are not available.


At this writing:

1. Only "/nevPDup.properties" has been tested, corresponding to a 
DarwinCore Archive of plant specimen data from the New England Vascular Plants data.

2. Configuration property files must reside in src/main/resources and correspond 
to the model of nevpDup.properties.



Bob Morris
November 2, 2014


