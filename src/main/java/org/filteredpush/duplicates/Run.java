package org.filteredpush.duplicates;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;
import org.filteredpush.duplicates.vectorize.EventDateVectorizer;
import org.filteredpush.duplicates.vectorize.OccurrenceVectorizer;
import org.filteredpush.duplicates.vectorize.Vectorizer;


public class Run {
	static final String hdfwrite = "hdfwrite";
	static final Vectorizer[] vectorizers = { new OccurrenceVectorizer(), new EventDateVectorizer() };


	public Run(String configFileName) {
	//	URL url = this.getClass().getResource(configFileName);
	//	 configFile = new File(url.getFile());

	}

	public static void main(String[] args) throws Exception{
		
		Options options = new Options();
		options.addOption("configFileName",true, " configuration java properties file");
		options.addOption("vectorize", false, " vectorize and write on hdfs");
		options.addOption("cluster", false, "run clusterer");
		options.addOption("dumpClusters", false, "dump clusters as ascii CVS");
		options.addOption("interpretClusters", false, "interpret non-trivial clusters as ascii CVS");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		}

		String configFileName = null;
		String runJob = null;
		InputStream inputStream = null;
		//try {
			
			if (cmd.hasOption("configFileName")) {
				 configFileName = cmd.getOptionValue("configFileName");
				 
				 inputStream= Run.class.getResourceAsStream(configFileName);

				System.err.println(configFileName);
				System.err.println("In Run, inputStream is: "+ inputStream);
			}
	
		//if we get here configFile must have been set
				Config cfg = new Config();
				cfg.configure(inputStream);
				System.err.println("WorkingDir: "+ cfg.getWorkingDir());
		 if (cmd.hasOption("vectorize")) {
			 //should check for which config is here and switch on it
			 //For now assume its NEVP
			 	System.err.println("In Run, vectorizing");
			 	NEVP2hdfs hdfsWriter = new NEVP2hdfs(cfg, Arrays.asList(vectorizers));
				hdfsWriter.execute();
		 } else if (cmd.hasOption("cluster")){
				DupDriver cluster = new DupDriver();
				int numClusters =cluster.execute(cfg);
				System.out.println("numClusters: " + numClusters);
		} else if (cmd.hasOption("dumpClusters")) {
			DupClusterDumper dumper = new DupClusterDumper();
			dumper.execute(cfg);
		}else if (cmd.hasOption("interpretClusters")) {
			ClusterInterpreter interpreter = new ClusterInterpreter();
			interpreter.execute(cfg);
		}
		 
	}
}
		
	

