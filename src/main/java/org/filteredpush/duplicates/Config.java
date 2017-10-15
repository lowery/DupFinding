package org.filteredpush.duplicates;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
	private   String workingDir = null; //where all resources are
	private  String recordID = null; //occurrence file header string for the record ID
	private  String maxInterpretedRecords = null;  //should test for being an integer...
	private  char interprSeparator = ',';
	private  char occurrenceSeparator = ',';
	private  char quoteChar = '\u0022';
	private  boolean useSmall = true;

	
	public Config() {
		
	}
	public void configure(InputStream inputStream) throws IOException { //might as well pass in a Properties object
		Properties prop = new Properties();
		
		prop.load(inputStream);
		
		workingDir = prop.getProperty("workingDir");
		recordID = prop.getProperty("recordID");
		maxInterpretedRecords = prop.getProperty("maxInterpretedRecords");
		interprSeparator = prop.getProperty("interprSeparator").charAt(0);
		occurrenceSeparator = prop.getProperty("occurrenceSeparator").charAt(0);
		quoteChar = prop.getProperty("quoteChar").charAt(0);
		useSmall = prop.getProperty("useSmall").contentEquals("true");

	}
	public String getWorkingDir() {
		return workingDir;
	}
	public String getRecordID() {
		return recordID;
	}
	public String getMaxInterpretedRecords() {
		return maxInterpretedRecords;
	}
	public char getInterprSeparator() {
		return interprSeparator;
	}
	public char getOccurrenceSeparator() {
		return occurrenceSeparator;
	}
	public char getQuoteChar() {
		return quoteChar;
	}
	public boolean isUseSmall() {
		return useSmall;
	}	
	
}
