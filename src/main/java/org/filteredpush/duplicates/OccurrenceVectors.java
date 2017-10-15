package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.joda.time.DateTime;



import au.com.bytecode.opencsv.CSVParser;

public class OccurrenceVectors {
	static final String COLLECTION_DATE = "collectionDate";
	
	public HashMap<String, Integer>  vectorize(InputStream in, String vectorFile) throws Exception {
		HashMap<String, Integer> labelMap = null;
		

		char quotechar = '"';
		char separator = ',';
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		GbifMetadata meta = new GbifMetadata(reader, separator, quotechar);
	//	Assert.assertNotNull(reader);

		//System.out.println(reader);
		// line = reader.readLine(); // should be labels
		// GbifMetadata meta = new GbifMetadata(reader); //which reads the first
		// line and sets the labelMap
		// System.out.println(meta);
		// assert (meta != null);
		labelMap = meta.getlabelMap(); // ADVANCES READER BY FIRST LINE
		// System.out.println("labelMap size: " + labelMap.size());
		// System.out.println("labelMap = " + labelMap);

		int idIdx = labelMap.get("Specimen_ID");
		// assert (idIdx == 0); // it should be the first column
		//Assert.assertNotNull(labelMap);

		int inCollectorNameIdx = labelMap.get("collectorName");
		//collectionDate is a synthetic attribute
		int inCollectionYearIdx = labelMap.get("From_Date_Year");
		int inCollectionMonthIdx = labelMap.get("From_Date_Month");
		int inCollectionDayIdx = labelMap.get("From_Date_Day");
		
		String outCollectionDateString = null;
		int inCollectorNumberIdx = labelMap.get("collectorNumber");
		int localityIdx = labelMap.get("locality");
		
		// System.out.println("collectorIdx:"+ collectorIdx
		// +" collectorNumberIdx:"+collectorNumberIdx+" localityIdx:"+localityIdx);

		

		int[] inFeatureIndex = { inCollectorNameIdx, inCollectorNumberIdx, inCollectionYearIdx, inCollectionMonthIdx, inCollectionDayIdx,  localityIdx}; //collectorIdx, collectorNumberIdx, localityIdx 
		String[] inFeatureString = { 
				"collectorName","collectorNumber",  "From_Date_Year","From_Date_Month","From_Date_Day", "locality"}; //"collectorNumber", "collectorName", "locality" };
		int numInFeatures = inFeatureIndex.length; // number of features to vectorize
		//Assert.assertTrue(numInFeatures == inFeatureIndex.length);
		//SET FEATURE STRING; We synthesize ISO dates from Year, Month, Day data of collection event
			
		String[] outFeatureString  = {"collectorName", "collectorNumber", "collectionDate", "locality"};
		int numOutFeatures = outFeatureString.length;
		HashMap<String, Integer> outFeatureMap = new HashMap<String, Integer>();
		for (int j = 0; j< outFeatureString.length; j++) 
			outFeatureMap.put(outFeatureString[j], j);
		
	//	String[] dateStrBuf = new String[3];
		
		// iterate over input
		// Occurrence record = null; // next record from input

		NamedVector occurrence = null;
		List<NamedVector> occurrences = new ArrayList<NamedVector>();
		long inCount = 0;
		long inCountInterval = 1;
		//reader.readLine();
		
		// ASSUMES DATE data is ints!
		CSVParser parser = meta.getParser();
		//PRECONDITION: first line contains labels and has already been read in setting up the labelMap
		String line = null;
		while (( line = reader.readLine()) != null) {
			// String buf[] = line.split(","); //should parameterize split str
			String buf[] = parser.parseLine(line);
			//String[] vecStr = new String[numOutFeatures]; // make a new one each time??
			double[] vec = new double[numOutFeatures]; // NEXT FILL IT!
			// precondition buf[0] = local data record id
			String yearStr = null; int year = 1970;  //DateTime default
			String monthStr = null; int month = 1;  //DateTime default
			String dayStr = null; int day = 1;  //DateTime default
			int dateStringsSeen=0;
			int outFeature = 0;
			for (int j = 0; j < numInFeatures; j++) {
				double value = 0.0;
				String str = inFeatureString[j];
				//System.out.println("j="+j + " str=" + str);
				//System.out.println(labelMap);
				//System.out.flush(); System.exit(0);
				String data = buf[labelMap.get(str)];  //get the buffer entry corresponding to the input feature we see
				boolean b = "collectorName".equals(str)
						|| "collectorNumber".equals(str)
						|| "locality".equals(str);
				   
				if (b) { // MORE
					
					value = VectorizeGBIFOccurrences.vectorize(data,
							VectorizationAlgorithm.JARO_WINKLER); 
					int outIdx = outFeatureMap.get(str);
						vec[outIdx] = value;
						System.out.println(buf[0] + ":: j="+j + " str= " + str + " data= "+data + " value="+value);
						
					//	System.out.println("data "+ data+ " should be data for j= " + j + " which should be " + "labelMap " + labelMap.get(str));
					//	System.out.println("str "+ str+ " has outIdx  " + outIdx + " in outFeatureMap" + " value "+value);
						
					// item
				} else if ("From_Date_Year".equals(str)){
					yearStr = data;
					dateStringsSeen += 1;
				}
				else if ("From_Date_Month".equals(str)) {
					month = Integer.parseInt(data);  
					if (month == 0) month = 1; // data absent, so punt DateTime is 1...12
					dateStringsSeen += 1;
				}
				else if ("From_Date_Day".equals(str)){
					day = Integer.parseInt(data);  
					if (day == 0) day = 1; //data is absent, so punt.  DateTime is 1...31
					dateStringsSeen += 1;
				}
					// item
				else if (1 == 2) {
					// place holder for georef
				} else {
					System.err.println(str + " " + data + " "
							+ "No vectorization Algorithm found.");// throw an exception: Algorithm class not found
				}
				if (dateStringsSeen ==3 ){
					dateStringsSeen = 0; // 
				
					DateTime dt = new DateTime(year, month, day, 0,0,0,0);
					value = VectorizeGBIFOccurrences.vectorize(dt);
					vec[outFeatureMap.get(COLLECTION_DATE)] = value;
					//System.out.println(COLLECTION_DATE + " " +outFeatureMap.get(COLLECTION_DATE));
				}

				//vec[outFeature++] = value;
				// vec[j] = buf[featureIndex[j]];

			}
			
			occurrence = new NamedVector(new DenseVector(vec), buf[0]);
			occurrences.add(occurrence);
			if ((inCount % inCountInterval) == 0) {
				System.out.println("inCount: " + inCount++ + " vector: " + occurrence.toString() + " size=" + occurrence.size());
			}
		}
		System.err.println("num occurrences: " + occurrences.size());

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path path = new Path(vectorFile);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				IntWritable.class, VectorWritable.class);
		VectorWritable outVec = new VectorWritable();
		long outCount = 0;
		long outCountInterval = inCountInterval;
		for (NamedVector vector : occurrences) {
			outVec.set(vector);
			writer.append(new IntWritable(Integer.parseInt(vector.getName())),
					outVec);
			System.out.println(outVec.toString());
			if ((outCount++ % outCountInterval) == 0) {
				//System.out.println("outCount: " + outCount++);
			}
		}
		writer.close();
		System.out.println("Done writing "+outCount+" vectors");
		
	return labelMap;	
	}
	
}
