package org.filteredpush.duplicates;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class DupFinder {
	///private boolean recluster = false;
	private String datasetDir = null;
	
	/**
	 * 
	 */
	public DupFinder(String datasetDir) {
		this.datasetDir = datasetDir;  
	}

	public int dumpCandidates(boolean recluster) throws Exception { //returns the number of non-unitary clusters
		String canopyInputFile =  datasetDir+"vectors"; 
		String canopyOutputDir = datasetDir+"clusters/";		
		String dumperInputDir =	canopyOutputDir+"clusters-0-final/";	
		String dumperOutputFile = datasetDir+"clusters.txt";
		String dumperInputFile = dumperInputDir+"part-r-00000"; //  /tmp/clusters/clusters-0-final/
	
		int candidateDups = 0; int outCount = 0; 
		System.err.println("DupFinder:\n"
				+ "datasetDir="+datasetDir
				+ " canopyInputFile="+canopyInputFile
				+ " canopyOutputDir="+canopyOutputDir
				+ " dumperInputDir="+dumperInputDir
				+  " dumperOutputFile="+dumperOutputFile
				+ " dumperInputFile="+dumperInputFile);
	
		//	"-dm", "org.apache.mahout.common.distance.CosineDistanceMeasure", "-t2", "0.0000000000000001", "-t1","0.00001",// "0.6",//between 0 and 1  

		String[] args = {"-i", canopyInputFile, "-o", canopyOutputDir, 
				 "-dm", "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure", "-t1", ".00000002", "-t2", ".00000001", 
						"-ow", "-cl"};
			 // "-t1", ".00000002", "-t2", ".00000001"  5570 clusters; 107 at 0 distance;  5455 unitary
		CanopyDriver driver = new CanopyDriver();
		if (recluster)	 driver.run(args);
					 //String[] dumpArgs = {"-i", dumperInputDir, "-o", dumperOutput, "-dt", "text", "-d", "/tmp/dictionary.txt", "-p", pointsDir};
		//String[] dumpArgs = {"-i", dumperInputFile, "-o", dumperOutputFile,  "-p",pointsDir};
	/*	System.err.println("dumpArgs:\n"
						+ " dumperInputFile="+dumperInputFile
						+  " dumperOutputFile="+dumperOutputFile);
	
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
				
	
		SequenceFile.Reader clusterReader = new SequenceFile.Reader(fs,
						new Path(dumperInputFile), conf);
			//Now get interpreter from reader
		IntWritable key = new IntWritable();
		VectorWritable value = new VectorWritable();
		while (clusterReader.next(key, value)) {  //key = clusterid, value = vectorid
			//Object outStr = key.toString()	+ " " + 
			//		value.get();//.getDelegate().asFormatString();
			
			System.out.println(key.toString() + " " +
					 value.get().asFormatString());
			outCount++;
		}
		clusterReader.close();
		*/
		return outCount;
		
		}
	}
	
