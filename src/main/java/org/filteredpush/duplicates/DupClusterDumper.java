package org.filteredpush.duplicates;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class DupClusterDumper {
	private String workDir = "";  //default to current directory ?
	public DupClusterDumper() {
	}

	public void execute(Config cfg) {
		//String workDir = "/home/ram/Dups/Lichens/";
		String workDir = cfg.getWorkingDir();
		EuclideanDistanceMeasure edm = new EuclideanDistanceMeasure();
		String clusterFile = workDir + "clusters/clusters-0-final/part-r-00000";
		String inputDir = workDir + "clusters/clusters-0-final";
		String pointsDir = workDir + "clusters/clusteredPoints";
		String outputFile = workDir + "simpleClusters.txt";
		String distanceMeasure = "EuclideanDistanceMeasure";
		Configuration conf = new Configuration();
		System.err.println("DupClusterDumper outputFile:" + outputFile);
		String[] cd_args = { "--input", inputDir, "--output", outputFile,
				"--outputFormat", "CSV", "--pointsDir", pointsDir, "-d",
				workDir + "dictionary.txt" , "-dt", "text"};
		try {
			ClusterDumper.main(cd_args);  //mahout ClusterDumper
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
