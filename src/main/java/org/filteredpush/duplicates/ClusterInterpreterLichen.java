package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import au.com.bytecode.opencsv.CSVParser;

public class ClusterInterpreterLichen {
	BufferedReader clusterReader = null; //file of cleaned clusters: clusterID,occurrence1ID, <occurrenceID>
	BufferedWriter interpWriter = null; //interpreted clusters: file with complete occurrence preceded by clusterID
	BufferedReader occurrenceReader = null;
	HashMap<Integer, String> occurrences = null;
	
	char clusterSeparator = '\t';//CSVParser.DEFAULT_SEPARATOR;
	char interpSeparator = '\t';
	char occurrenceSeparator = '\t';
	char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
	CSVParser parser = null;
	String headers=null;
	HashMap<Integer, String> records = new HashMap<Integer, String>(40000);

	public ClusterInterpreterLichen(BufferedReader clusterReader, char clusterSeparator, BufferedWriter interpWriter, char interpSeparator, BufferedReader occurrenceReader, char occurrenceSeparator)
			throws IOException { // should deal with this since reader is
									// coupled to caller
		// build a labelMap based on a header line extracted from a CSV file
		this.clusterReader = clusterReader; this.interpWriter = interpWriter; this.occurrenceReader = occurrenceReader;
		this.clusterSeparator = clusterSeparator; this.interpSeparator = interpSeparator;
		String line=null;
	
		//prepare the recordsMap, keyed by id, value is remaining string. needed for search
		headers = occurrenceReader.readLine(); // skip first line in making the map. It's "occid"; use it in interpret()
		//writer.write("clsID"+separator+headers);
		int count=0;
		
		while ((line = occurrenceReader.readLine()) != null) {
		
			//StringTokenizer st = new StringTokenizer(line, "\t");
			int firstSep = line.indexOf("\t"); //separator within occurrence items
			Integer recID = new Integer(line.substring(0,firstSep)); //fails if  recID is not an integer....
			String record = line.substring(firstSep+1);  //rest of record without iD
			records.put(recID, record);
			count++;
		}
		System.out.println(count);
		parser = new CSVParser(clusterSeparator, quotechar);
	}
	
	int interpret() throws IOException {
		String line = null;
		//String header = reader.readLine(); 
		int count = 0; //num of non-trivial clusters
		interpWriter.write ("clstID"+interpSeparator+headers); interpWriter.append('\n');
		while ( ( line = clusterReader.readLine()) != null) {
			String buf[] = parser.parseLine(line);
			String clusterID = buf[0];

			//find and write the next record in cluster provided at least two records
			if (buf.length > 2) { // skip trivial clusters.
				count++;
				for (int i = 1; i < buf.length; i++) {
					String recordIDstr = buf[i];
					Integer recordID = new Integer(recordIDstr);
					String record = records.get(recordID);
					interpWriter.write(clusterID + occurrenceSeparator
							+ recordID + occurrenceSeparator + record + "\n");
				}
				interpWriter.append("foobar\n");
			}
		}
		clusterReader.close();
		interpWriter.close();
		return count;
	}

	private String getRecord(String recordID) {
		String str = records.get(recordID);
		return  str;
	}

}
