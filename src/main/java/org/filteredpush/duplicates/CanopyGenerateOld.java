package org.filteredpush.duplicates;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.AbstractCluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class CanopyGenerateOld {

	private String datasetDir;
	private String inputDir;
	private String inputFile;
	
	public CanopyGenerateOld() {
		// TODO Auto-generated constructor stub
	}

	
	public void generate(String datasetDir, String inputDir,  boolean recluster) throws Exception {
		this.datasetDir = datasetDir ; // bbg, Rubiaeceae/ or fungi/ for now
		this.inputDir =  inputDir + datasetDir;
		this.inputFile =  inputDir + "vectors"; // input,
																			
		String outputDir = "output/"+datasetDir;
		Path vectorsPath = new Path(inputFile);
		Path outputPath = new Path(outputDir);
		if (recluster) 	HadoopUtil.delete(new Configuration(), outputPath);
		double t1 = 5.0; double t2 = 0.001;
		EuclideanDistanceMeasure measure = new EuclideanDistanceMeasure();
		try {
		
		
			//Job.run(vectorsPath, outputPath, measure, t1, t2);
			Configuration conf=new Configuration();
			System.out.println("Ready to recluster: "+ vectorsPath.getParent().toString() + " " + outputPath.toString()); System.out.flush();
			//System.exit(1);
			if (recluster) CanopyDriver.run(conf, vectorsPath,
				        outputPath, measure, t1, t2, true, 0.0, true);  //last arg false for MapReduce, true for runSequential
			ClusterDumper dumper = new ClusterDumper(new Path(outputPath,
			      "clusters-0-final"), new Path(outputPath, "clusteredPoints"));
			 Path pointsPathDir = new Path(outputPath, "clusters-0-final/part-r-00000");
			 dumper.printClusters(null);
			 Map<Integer, List<WeightedPropertyVectorWritable>> clusterMap = dumper.getClusterIdToPoints();
			 
			File dumpOut = new File("clusterdump.txt"); dumpOut.delete();
			PrintStream out = new PrintStream(dumpOut);
			PrintWriter writer = new PrintWriter(dumpOut);
			
			 Iterator clusters = clusterMap.entrySet().iterator();
			 System.out.println("cluster count:"+clusterMap.size());
			 int tooManyVectors = 10000;
			 while (clusters.hasNext()) {
				 Map.Entry<Integer, List<WeightedVectorWritable>> entry = (Map.Entry) clusters.next();
				 Integer clusterID = entry.getKey();
				 List<WeightedVectorWritable> cluster = entry.getValue();
				 int size = cluster.size();
				 if ( size  <= tooManyVectors) {
					// writer.println(clusterID + "::"+ cluster);
					 Iterator clusterIter = cluster.iterator();
					 while (clusterIter.hasNext()) {
						 WeightedVectorWritable nextVector = (WeightedVectorWritable) clusterIter.next();
						 Vector vector = nextVector.getVector();
						out.print (vector == null ? "null" : AbstractCluster.formatVector(vector, null));
						// out.print("v= "+nextVector+ " ");
					 }
					 out.print("\n");
				 }
			 }
		}
				 //else out.println(clusterID + "::" + size + " vectors");
				 
				
			//	 System.out.println("cluster "+ clusterID + ":" + cluster.size()+ " vectors");
			//		 writer.println(cluster);
			// }
			// writer.close();
			//} 
		//	 writer.println(clusterMap);
		//	 System.out.println(clusterMap.size()+ " " + dumper.getMaxPointsPerCluster() + " " + dumper.getTermDictionary() );
			 
		/*	 Map<Integer,List<WeightedVectorWritable>> clusterMap = 
					 ClusterDumper.readPoints(pointsPathDir, 20, conf);
			 System.out.println("maxPointsPerCluster:"+dumper.getMaxPointsPerCluster() 
					 +" numTopFeatures: "+dumper.getNumTopFeatures()+ " termDict:" + dumper.getTermDictionary());
			 
			//   dumper.printClusters(null);
			 System.out.println(clusterMap.toString()+ " "+ clusterMap.getClass() + " size: " + clusterMap.size());
			// Configuration conf = new Configuration();
		//	 FileSystem fs = FileSystem.get(conf); 
		/*	 SequenceFile.Reader reader = new SequenceFile.Reader(fs, 
						new Path(outputPath, "clusters-0-final/part-r-00000"),conf); 
				IntWritable key = new IntWritable();
				WeightedVectorWritable value = new WeightedVectorWritable();
				while (reader.next(key, value)) {
					System.out.println(value.toString() + " in cluster " +key.toString() );	
					}
			
				reader.close(); */
		/*	Path theClusters = new Path(outputPath, "clusters-0-final/part-r-00000");
		List<List<Cluster>> clusters = ClusterHelper.readClusters(new Configuration(), theClusters);
			System.out.println("clusters.size = "+clusters.size()+"\n" + clusters.toString());
			Iterator<List<Cluster>> clustersIter = clusters.iterator();
			while ( clustersIter.hasNext()) {
				List<Cluster>cluster =  clustersIter.next();
				Iterator<Cluster> clusterIter = cluster.iterator();
				while (clusterIter.hasNext()) {
					Object next = clusterIter.next();
					System.out.println( " is "+((Canopy)next).asFormatString() + " " + next.getClass());
				}
				
				
			} */
		//	System.out.println(Cluster.CLUSTERED_POINTS_DIR + " " + Cluster.CLUSTERS_DIR + " " + Cluster.FINAL_ITERATION_SUFFIX);
		catch (Exception e) {
			System.out.println("test caught Exception");
			System.out.println(outputDir+"clusters-0-final/"+"part-r-00000");
			e.printStackTrace(System.out);
		}
	}
}

