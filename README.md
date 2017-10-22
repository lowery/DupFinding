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


Among other properties, DupFinding/src/main/resources/nevpDup.properties must contain
the value of a writable  working directory (property workingDir). In the $workingDir
should reside a CSV file occurrences.small.csv containing a small set of occurrence
records in CSV form, whose first line comprises headers from Darwin Core properties.
the file occurrences.csv whichich is used if useSmall = false.

It's important to note that what is here only builds the input to Hadoop MapReduce and
I have not yet tried to deploy Hadoop in part because I used a fair bit of deprecated
Hadoop. The bold may try to study and get it working following
https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html. I will try---but not
with great hurry---to make current Hadoop work.

What does Hadoop plus this project get one:

The code in class VectorizeGBIFOccrences carries
invocations of three vectorization algorihms of
strings:
JARO_WINKLER and LEVENSTEIN distances between strings
that are names of headers of columns in CSV files
of the occurrence data
are well known as string distances in data mining.
The third, DATE_TIME, vectorizes date-time item as
described in class DateToMillis. It is the number of
milliseconds from the beginning of 1970 GMT. It can
be negative.  Finally, class VectorizeGBIFOccrences
holds an unimplemented vectorization for
georeferencing.

The software produces two sets of clusters:
(a)simpleClusters.txt and (b)interpretedClusters.txt.
The former is most suitable for further software analysis,
the latter for human investegation.  Both can generally
be read by spreadsheets.

simpleClusters.txt has

interpretedClusters.txt has a header line generally self
explanatory to those familiar with DarwinCore, although not
all exactly DwC. The first column contains a generated clusterID
(or the word "cluser" as a seperator) Each row contains an element
of the cluster, the occurrence id of which is in the second
column. In a spreadsheet it easy to see  that cluster 19
in interpretedClusters_2017100217.txt has over 800 alleged
botanical duplicates.  Not only from the size of such a set,
but also 



Bob Morris
17 Oct 2017
initially Nov 2, 2014


