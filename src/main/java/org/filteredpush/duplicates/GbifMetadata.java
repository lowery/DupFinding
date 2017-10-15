package org.filteredpush.duplicates;

import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import au.com.bytecode.opencsv.CSVParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.File;
import java.io.IOException;

public class GbifMetadata {

	HashMap<String, Integer> labelMap = null; // for now just <k,v> pair <dwcTermName, index>;
									// maybe model all of meta.xml using jaxb?
	BufferedReader reader = null;
	CSVParser parser = null;
/**
 * 
 * @param reader a BufferedReader
 * @throws IOException
 */
	GbifMetadata(BufferedReader reader) throws IOException {
		this(reader, CSVParser.DEFAULT_SEPARATOR, CSVParser.DEFAULT_QUOTE_CHARACTER);
	}
	GbifMetadata(BufferedReader reader, char separator) throws IOException {
		this(reader, separator,  CSVParser.DEFAULT_QUOTE_CHARACTER);
	}
	public GbifMetadata(BufferedReader reader, char separator, char quotechar) throws IOException { //should deal with this since reader is coupled to caller
		//build a labelMap based on a header line extracted from a CSV file
		this.reader = reader;
		parser = new CSVParser(separator, quotechar);
		
	} 
	

	/** 
	 * 
	 * @param metadataDir path to directory containing the GBIF Occurrence metadata
	 * @param metadataFile XML file containing metadata. Typically "meta.xml"
	 * @throws Exception
	 * In version 0.9 of the GBIF Occurrence service, the Occurence records are
	 * presented as a DarwinCore Archive. The records are in a CSV file named
	 * occurrence.txt whose first line is the column headers.  Alas, the strings
	 * in this line bear only scant relation to the attribute URIs in meta.xml
	 * 
	 */
		@Deprecated
		GbifMetadata(String metadataDir, String metadataFile) throws Exception {
		File xmlMetadata = new File(metadataDir, metadataFile);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		labelMap = new HashMap<String, Integer>();
		try {
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlMetadata);
			NodeList fieldlist = doc.getElementsByTagName("field");
			for (int j = 0; j < fieldlist.getLength(); j++) {
				Node node = fieldlist.item(j);
				if (node.getNodeType() == Node.ELEMENT_NODE) { // it better be...
					Element fieldElement = (Element) node;
					String uri = fieldElement.getAttribute("term"); 
					String key = FilenameUtils.getName(uri);						// now parse off last part
					
					Integer value = Integer.parseInt(fieldElement.getAttribute("index"));
					labelMap.put(key, value);
					
				}
			}
		} catch (Exception e) {
			// log Exception and rethrow
			throw e;
		}
	}

	
	public HashMap<String, Integer> getlabelMap() throws IOException{
		String line = reader.readLine(); //ought to be the first line.  Any way to check this???
		//String[] labels = line.split(",");
		String[] labels = parser.parseLine(line);
		//System.err.println("labels.length: "+labels.length);
		// read the first line and make keys for attribute name
		labelMap = new HashMap<String,Integer>();
		for (int i = 0; i < labels.length; i++) {
			labelMap.put(labels[i], i);
		//	System.out.println(i +" "+ labels[i]);
		}
		//System.err.println("labelMap size: "+ labelMap.size());
		return labelMap;
	}

	public void setlabelMap(HashMap<String, Integer> labelMap) {
		this.labelMap = labelMap;
	}
	public BufferedReader getReader() {
		return reader;
	}
	public void setReader(BufferedReader reader) {
		this.reader = reader;
	}
	public CSVParser getParser() {
		return parser;
	}
	public void setParser(CSVParser parser) {
		this.parser = parser;
	}
	

}
