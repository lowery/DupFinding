package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;


import au.com.bytecode.opencsv.CSVParser;

public class ClusterCleaner {
	BufferedReader reader = null;
	BufferedWriter writer = null;
	char separator = CSVParser.DEFAULT_SEPARATOR;
	char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
	CSVParser parser = null;

	
	ClusterCleaner(BufferedReader reader, BufferedWriter writer) throws IOException {
		
		this(reader, writer, CSVParser.DEFAULT_SEPARATOR,
				CSVParser.DEFAULT_QUOTE_CHARACTER);
		
	}

	ClusterCleaner(BufferedReader reader, BufferedWriter writer, char separator) throws IOException {
		this(reader, writer, separator, CSVParser.DEFAULT_QUOTE_CHARACTER);
	}

	public ClusterCleaner(BufferedReader reader, BufferedWriter writer, char separator, char quotechar)
			throws IOException { // should deal with this since reader is
									// coupled to caller
		// build a labelMap based on a header line extracted from a CSV file
		this.reader = reader; this.writer = writer;
		parser = new CSVParser(separator, quotechar);

	}
	
	int clean(BufferedReader reader, BufferedWriter writer) throws IOException {
		String line = null;
		int count = 0; //num of non-trivial clusters
		while ( (line = reader.readLine()) != null) {
			String buf[] = parser.parseLine(line);
			if (buf.length >2 ) {
				count++;
				writer.write(line);
				writer.append('\n');
				
			}
		}
		reader.close();
		writer.close();
		return count;
	}

}
