package org.filteredpush.duplicates;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.CSVClusterWriter;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.common.distance.DistanceMeasure;

public class CSVClusterDump {
	private BufferedWriter bw = null;
	private Map<Integer,List<WeightedPropertyVectorWritable>> clusterIdToPoints = null;
	private DistanceMeasure dm;
	
	CSVClusterDump(BufferedWriter bw, Map<Integer,List<WeightedPropertyVectorWritable>> clusterIdToPoints, DistanceMeasure dm) {
		this.bw = bw;
		this.clusterIdToPoints = clusterIdToPoints;
		this.dm = dm;
	}
	
	static void dump(String[] argv) throws IOException {
		

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//String vectorFile = "occurrenceVectors/" + datasetDir + "vectors";
		String vectorFile= "/tmp/vectors";
		Path path = new Path(vectorFile);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				IntWritable.class, VectorWritable.class);
		VectorWritable outVec = new VectorWritable();
		long outCount = 0;
		long inCountInterval = 0;
		long outCountInterval = inCountInterval = 1000;
	/****
		for (NamedVector vector : ) {
			outVec.set(vector);
			writer.append(new IntWritable(Integer.parseInt(vector.getName())),
					outVec);
			//System.out.println(outVec.toString());
			if ((outCount++ % outCountInterval) == 0) {
				//System.out.println("outCount: " + outCount++);
			}
		}
		
		writer.close();
	****/
		System.out.println("Done writing "+outCount+" vectors");
		// check file was written correctly
		SequenceFile.Reader vectorReader = new SequenceFile.Reader(fs,
				new Path(vectorFile), conf);

		IntWritable key = new IntWritable();
		VectorWritable value = new VectorWritable();
		outCount = 0; outCountInterval = 1;
		boolean check = true;
		if (check) {
			while (vectorReader.next(key, value)) {
				if ((outCount % outCountInterval) == 0) {
					Object outStr = key.toString()	+ " " + 
				(NamedVector) value.get();//.getDelegate().asFormatString();
					System.out.println(//key.toString() + " " +
						 value.get().asFormatString());

				//System.out.println(outStr);
				}
				outCount++;
			}
			vectorReader.close();
		}
		System.out.println("Done reading " + outCount +" records");

		try {
			BufferedWriter bw;
			conf = new Configuration();
			fs = FileSystem.get(conf);
			File pointsFolder = new File(argv[0]);
			File files[] = pointsFolder.listFiles();
			bw = new BufferedWriter (new FileWriter (new File(argv[1])));
			
			
			
			
		/*	 Map<Integer,List<WeightedPropertyVectorWritable>> clusterIdToPoints = xx;
			CSVClusterWriter clusterWriter = new CSVClusterWriter(bw,clusterIdToPoints, measure);
			//iterate over the clusters, then the list
			for (Integer key: clusterIdToPoints.keySet()) {
				List<WeightedPropertyVectorWritable>> cluster = clusterIdToPoints.get();
			}
		*/	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	static void main(String[] argv ) {
	}

	public BufferedWriter getBw() {
		return bw;
	}

	public void setBw(BufferedWriter bw) {
		this.bw = bw;
	}

	public Map<Integer, List<WeightedPropertyVectorWritable>> getClusterIdToPoints() {
		return clusterIdToPoints;
	}

	public void setClusterIdToPoints(
			Map<Integer, List<WeightedPropertyVectorWritable>> clusterIdToPoints) {
		this.clusterIdToPoints = clusterIdToPoints;
	}

	public DistanceMeasure getDm() {
		return dm;
	}

	public void setDm(DistanceMeasure dm) {
		this.dm = dm;
	}
	}

