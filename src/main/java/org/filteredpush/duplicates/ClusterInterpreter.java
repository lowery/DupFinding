package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;


import au.com.bytecode.opencsv.CSVParser;
public class ClusterInterpreter {
	HashMap<Integer, String> records = null;
//	BufferedReader clusterReader = null; //file of cleaned clusters: clusterID,occurrence1ID, <occurrenceID>
//	BufferedWriter interpWriter = null; //interpreted clusters: file with complete occurrence preceded by clusterID
	//BufferedReader occurrenceReader = null;
	//HashMap<Integer, String> occurrences = null;
	
	

//	char occurrenceSeparator;
//	char quotechar;
	//CSVParser parser = null;
	String headers=null;
	
	public ClusterInterpreter () {
		
	}
	
	int execute(Config cfg) throws IOException {
		
		//Config cfgtst = new Config("nevpDup.properties");
		//cfgtst.configure();
		//String dir="/tmp/test/";
		String workingDir = cfg.getWorkingDir();
		char occurrenceSeparator= cfg.getOccurrenceSeparator();
		char interprSeparator=cfg.getInterprSeparator();
		//String clusterSeparator = "sep"; 
		
		String interpretedFile= workingDir+"interpretedClusters.txt"; 
		BufferedWriter interpWriter = new BufferedWriter(new FileWriter (new File (interpretedFile)));
		
		String clusterFile=workingDir+"simpleClusters.txt";  //uninterpreted dumped clusters with at least two records 
		BufferedReader clusterReader = new BufferedReader(new FileReader (new File (clusterFile)));
		
		String ocf = cfg.isUseSmall()?"occurrences.small.csv":"occurrences.csv";
		String occurrenceFile = workingDir+ocf;
		BufferedReader occurrenceReader = new BufferedReader(new FileReader (new File (occurrenceFile)));
		
		String clusterSeparator = "cluster"; //signifies end of interpreted cluster in CSV form 

	//	int numOccurrences = 40000;
		//HashMap<Integer, String> records = new HashMap<Integer, String>(numOccurrences);
		HashMap<Integer, String> records = new HashMap<>();
		//fill it with the occurrenceReader:
		int numOccurrences=0; //don't really need it since map is dynamic. But memory is linear with numOccurrences...BAD! Should us a DB
		
		 headers = occurrenceReader.readLine(); // first line is headers
		 String line = null;
		System.err.println("ClusterInterprter firstline: " + headers);
		while ((line = occurrenceReader.readLine()) != null) {
			//System.err.println("ClusterInterprter line: " + line);
			//StringTokenizer st = new StringTokenizer(line, "\t");
			
			int firstSep = line.indexOf(occurrenceSeparator); //separator within occurrence items
			Integer recID = new Integer(line.substring(0,firstSep)); //fails if  recID is not an integer....
			String record = line.substring(firstSep+1);  //rest of record without iD
			records.put(recID, record);
			numOccurrences++;
		}
		occurrenceReader.close();
		System.out.println(numOccurrences + " clusters");
			
		//ClusterInterpreter ccl = new ClusterInterpreter(clusterReader,clusterSeparator, interpWriter, interprSeparator, occurrenceReader, occurrenceSeparator );
	
		//System.err.println ("duplicate-sets=" + count);
		CSVParser parser = new CSVParser();
		line = null;

		//String header = reader.readLine(); 
		int dups = 0; //num of non-trivial clusters
		interpWriter.write ("clstID"+interprSeparator+headers); interpWriter.append('\n');
		while ( ( line = clusterReader.readLine()) != null) {
			String buf[] = parser.parseLine(line);
			String clusterID = buf[0];

			//find and write the next record in cluster provided at least two records
			if (buf.length > 2) { // skip trivial clusters.
				dups++;
				for (int i = 1; i < buf.length; i++) {
					String recordIDstr = buf[i];
					Integer recordID = new Integer(recordIDstr); //spurious null here somewhere
					String record = records.get(recordID);
					interpWriter.write(clusterID + interprSeparator
							+ recordID + occurrenceSeparator + record + "\n");
				}
				interpWriter.append(clusterSeparator+"\n");
			}
		}
		clusterReader.close();
		interpWriter.close();
		return dups;
	}
}
