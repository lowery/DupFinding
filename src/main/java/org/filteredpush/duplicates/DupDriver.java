package org.filteredpush.duplicates;



import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;



public class DupDriver {
	 static final String CONFIG_DEFAULT = "nevpDup.properties" ; // must be in local class directory
	
	public int execute(Config cfg) throws Exception {
		boolean recluster = true; //Always true if invoking dupFinder ???
		 

		//cfgtst.test();  //should make a Config object visible to DupFinder
		DupFinder dupFinder = new DupFinder(cfg.getWorkingDir());  //where the data are.  
		int numClusters = dupFinder.dumpCandidates(recluster);
		System.err.println(numClusters + " non unitary clusters");
		return numClusters;
	}

}
