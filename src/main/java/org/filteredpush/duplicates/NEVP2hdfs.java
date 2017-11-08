package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.joda.time.DateTime;

import au.com.bytecode.opencsv.CSVParser;

public class NEVP2hdfs {
    static final String COLLECTION_DATE = "eventDate";
    static final
	String[] inFeatureStringArray = { "family","genus", "scientificName", "recordedBy","recordNumber",  "eventDate", "locality","stateProvince"}; 
    int numInFeatures = inFeatureStringArray.length;

    public void execute(Config cfg) throws Exception {
	String line;

	String dataset = "occurrences" + (cfg.isUseSmall() ? ".small" : "") + ".csv"; // add postfix to use .small or .med test set 
	String occurrenceFileName =  cfg.getWorkingDir() + dataset;
	System.err.println("occurrenceFileName="+occurrenceFileName);
	//InputStream in = this.getClass().getClassLoader().getResourceAsStream(inputString);
	FileInputStream in = new FileInputStream(occurrenceFileName);
	char quoteChar = cfg.getQuoteChar();
	char separator = cfg.getOccurrenceSeparator();
		
	BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	GbifMetadata meta = new GbifMetadata(reader, separator, quoteChar);
	//Assert.assertNotNull(reader);
		
	HashMap<String, Integer> labelMap = null;
	//assert (meta != null);
	labelMap = meta.getlabelMap(); // ADVANCES READER BY FIRST LINE, which has headers
	System.out.println("labelMap size: " + labelMap.size());
	System.out.println("labelMap = " + labelMap);
	System.err.println("131: "+cfg.getWorkingDir() + " " + cfg.getRecordID());
	int idIdx = labelMap.get(cfg.getRecordID());
	assert (idIdx == 0); // it should be the first column
	//	Assert.assertNotNull(labelMap);
		
	int[] inFeatureIndex = new int[numInFeatures];
	for (int i = 0; i < numInFeatures; i++)  inFeatureIndex[i] = i;

		//calculate outFeatureMap.  Its inverse is the mahout dictionary
	String df = cfg.getWorkingDir()+"dictionary.txt";
		
	PrintStream dictionary = new PrintStream(new File(df));
	String[] outFeatureStringArray = new String[numInFeatures]; /* inFeatureString; */
	int numOutFeatures =  outFeatureStringArray.length;
	dictionary.println(numOutFeatures);
	HashMap<String, Integer> outFeatureMap = new HashMap<String, Integer>();
	for (int i = 0; i < numInFeatures; i++)  outFeatureStringArray[i] = inFeatureStringArray[i] ;		
	int j = 0;
	for ( j = 0; j< numOutFeatures; j++) {
	    outFeatureMap.put(outFeatureStringArray[j], j);
	    dictionary.println(outFeatureStringArray[j]+"\t"+ "1"+"\t"+j);	//term docFreq index tab separated
	}
	dictionary.close();
	
		// iterate over input
		// Occurrence record = null; // next record from input
	
	long inCount = 0;
	long inCountInterval = 1;
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	String vectorFile =  cfg.getWorkingDir() + "vectors";
	System.out.println("vectorFile="+vectorFile); //System.exit(1);
	Path path = new Path(vectorFile);
	SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, VectorWritable.class);
	VectorWritable outVec = new VectorWritable();
			
	CSVParser parser = meta.getParser();
	long outCount = 0;
	inCount = 0;
		//PRECONDITION: first line contains labels and has already been read in setting up the labelMap
	while ((line = reader.readLine()) != null) {
	    inCount++;
	    String buf[] = parser.parseLine(line);
	    double[] featureVec = new double[numOutFeatures]; // NEXT FILL IT!
			// precondition buf[0] = local data record id
	    boolean dataNull = false;
	    int outFeature = 0;

	    for (int inFeature = 0; inFeature < numInFeatures; inFeature++) {
		dataNull = true;
		double value = 0.0;
		String str = inFeatureStringArray[inFeature];
				//System.out.println("inFeature="+inFeature + " str=" + str);
				//System.out.println(labelMap);
				//System.out.flush(); System.exit(0);
		String data = buf[labelMap.get(str)];  //get the buffer entry corresponding to the input feature we see
		//		System.out.println(str+ " " + data);
		boolean b1 = true;			   
		if (b1) { // MORE
		    value = VectorizeGBIFOccurrences.vectorize(data, VectorizationAlgorithm.LEVENSTEIN); 
		    int outIdx = outFeatureMap.get(str);
		    featureVec[outIdx] = value;
		    dataNull = false;
		} else if ("eventDate".equals(str)){
		    try {
			DateTime dt = DateTime.parse(data);
			value = VectorizeGBIFOccurrences.vectorize(dt);
			featureVec[outFeatureMap.get(COLLECTION_DATE)] = value;
			dataNull = false;
		    }
		    catch (Exception e) {
			dataNull = true;
		    }
		}
							// item
		else if (1 == 2) {
		    // place holder for georef
		} 
		else {
		    System.err.println(str + " " + data + " "
				       + "No vectorization Algorithm found.");// throw an exception: Algorithm class not found
		}
	    }
	    if (!dataNull){
		DenseVector occurrenceVec = new DenseVector(featureVec);
		VectorWritable writable = new VectorWritable();
		writable.set(occurrenceVec);
		writer.append(new IntWritable(Integer.parseInt(buf[0])), writable);
		outCount++;
	    }
	}
	System.out.println("outCount="+outCount);
	long outCountInterval = inCountInterval;
		//OK TO HERE
	writer.close();
	System.out.println("Done writing "+outCount+" vectors");
		// check file was written correctly
	SequenceFile.Reader vectorReader = new SequenceFile.Reader(fs, new Path(vectorFile), conf);
	IntWritable key = new IntWritable();
	VectorWritable value = new VectorWritable();
	outCount = 0; outCountInterval = 1000;
	boolean check = true;
	if (check) {
	    while (vectorReader.next(key, value)) {
		if ((outCount % outCountInterval) == 0) {
				//	Object outStr = key.toString()	+ " " + 
				//			value.get();//.getDelegate().asFormatString();
				}
		outCount++;
	    }
	    reader.close();
	    vectorReader.close();
	    System.out.println("Checking readability of " + outCount +" records");
	}
    }
	
}
