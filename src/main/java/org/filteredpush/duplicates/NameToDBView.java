package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.HashMap;



import au.com.bytecode.opencsv.CSVParser;

public class NameToDBView {
	String idName = "occid";
	String[] feature = {idName, "recordedBy", "recordNumber", "locality", "eventDate" 
			,"occurrenceID", "scientificName", "omenid", "exsnumber", "ometid"};
	HashMap<String, Integer> smallLabelMap = null; 
	HashMap<String, Integer> labelMap = null;
	
	public HashMap<String, Integer> getSmallLabelMap() {
		return smallLabelMap;
	}

	public void setSmallLabelMap() {
		//initialize label row
	
		for (int j=0; j<feature.length; j++ ) {
			smallLabelMap.put(feature[j], j);
		}
		
	}
	public void setSmallLabelMap(HashMap<String, Integer> smallLabelMap) {
		this.smallLabelMap = smallLabelMap;
		
	}

	public NameToDBView() {
		super();
		// TODO Auto-generated constructor stub
	}
	//For now specialized to Lichens.  Methods invert SequenceFileLichensTest2

	public HashMap<String, Integer> smalLabelMap (InputStream occurrences, InputStream clusters) throws IOException{
		char quotechar = '"';
		char separator = '\t';
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(occurrences));
		GbifMetadata meta = new GbifMetadata(reader, separator, quotechar);
	//	Assert.assertNotNull(reader);
		
		
		//System.out.println(reader);
		// line = reader.readLine(); // should be labels
		// GbifMetadata meta = new GbifMetadata(reader); //which reads the first
		// line and sets the labelMap
		// System.out.println(meta);
		// assert (meta != null);
		labelMap = meta.getlabelMap(); // ADVANCES READER BY FIRST LINE, which has headers
		// System.out.println("labelMap size: " + labelMap.size());
		System.out.println("labelMap = " + labelMap);

		int idIdx = labelMap.get(idName);
		 assert (idIdx == 0); // it should be the first column
	//	Assert.assertNotNull(labelMap);
		
		//now make an in memory map from parser and return it
		CSVParser parser = meta.getParser();
		
		// get just what's needed
		for (int i = 0; i < feature.length; i++) {
			
			
		}
		/* 
		 * Output smallLabelMap headers;
		 * assume a List< List<String>> where each List<String> is a cluster with String as vector name.
		 *
		 * We don't care what the order of clusters or the order of names within each cluster
		 * usage: iterate over clusters. 
		 *     for each cluster, iterate over vectors. 
		 *       for each vector, output the small labelMap values for that vector
		 * 
		 * */
		
	
		BufferedReader clusterReader = new BufferedReader(new InputStreamReader(clusters));
		String vecId = null;
		
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream ("/tmp/dbout.csv"), "utf-8"));
		while ((vecId = clusterReader.readLine()) != null) {
			//should check it's an integer, else error, but maybe just ignore and log?
			if (!vecId.isEmpty()) out.write(dbout(vecId)); else out.write("\n"); 
			
		}
		
		return smallLabelMap;
	}
	String dbout(String vecId) {
		String dbForm = null;
		//assume a legitimate id
		//assume have a
		
		return dbForm;
	}
	

	public String[] getFeature() {
		return feature;
	}

	public void setFeature(String[] feature) {
		this.feature = feature;
	}
	
	public void compressOccurrenceData(String[] feature, File occurrence){
		//compress occurrence.txt to omit everything but the feature array entries
		//BufferedReader reader = new BufferedReader (occurrence)
		//while 
	}

}
