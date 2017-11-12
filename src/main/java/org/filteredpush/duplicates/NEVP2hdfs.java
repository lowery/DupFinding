package org.filteredpush.duplicates;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.filteredpush.duplicates.vectorize.Vectorizer;

import au.com.bytecode.opencsv.CSVParser;

public class NEVP2hdfs {
    private List<Vectorizer> vectorizers;
    private Config config;

    private BufferedReader reader;
    private GbifMetadata meta;

    private Map<String, Integer> labelMap;

    private LinkedList<String> features = new LinkedList<>();


    public NEVP2hdfs(Config config, List<Vectorizer> vectorizers) throws IOException {
        this.config = config;
        this.vectorizers = vectorizers;

        for (Vectorizer vectorizer : vectorizers) {
            features.addAll(Arrays.asList(vectorizer.listFeatures()));

            // Create dictionary file and write number of features to first line
            String df = config.getWorkingDir() + "dictionary.txt";

            PrintStream dictionary = new PrintStream(new File(df));
            dictionary.println(features.size());

            for (int i = 0; i < features.size(); i++) {
                // Write features to dictionary file
                dictionary.println(features.get(i) + "\t" + "1" + "\t" + i);    //term docFreq index tab separated
            }

            dictionary.close();
        }

        loadDataset();
        initLabelMap();
    }

    private void loadDataset() throws IOException {
        String dataset = "occurrences" + (config.isUseSmall() ? ".small" : "") + ".csv"; // add postfix to use .small or .med test set
        String occurrenceFileName = config.getWorkingDir() + dataset;
        System.err.println("occurrenceFileName=" + occurrenceFileName);

        //InputStream in = this.getClass().getClassLoader().getResourceAsStream(inputString);
        FileInputStream in = new FileInputStream(occurrenceFileName);
        char quoteChar = config.getQuoteChar();
        char separator = config.getOccurrenceSeparator();

        reader = new BufferedReader(new InputStreamReader(in));
        meta = new GbifMetadata(reader, separator, quoteChar);
        //Assert.assertNotNull(reader);
    }

    private void initLabelMap() throws IOException {
        //assert (meta != null);
        labelMap = meta.getlabelMap(); // ADVANCES READER BY FIRST LINE, which has headers

        System.out.println("labelMap size: " + labelMap.size());
        System.out.println("labelMap = " + labelMap);
        System.err.println("131: " + config.getWorkingDir() + " " + config.getRecordID());

        int idIdx = labelMap.get(config.getRecordID());
        assert (idIdx == 0); // it should be the first column
        //	Assert.assertNotNull(labelMap);
    }

    public void execute() throws Exception {
        // Load vectors file for writing
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String vectorFile = config.getWorkingDir() + "vectors";
        System.out.println("vectorFile=" + vectorFile); //System.exit(1);
        Path path = new Path(vectorFile);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, VectorWritable.class);

        CSVParser parser = meta.getParser();

        long outCount = 0;
        String line;
        // iterate over input
        // Occurrence record = null; // next record from input

        //PRECONDITION: first line contains labels and has already been read in setting up the labelMap
        while ((line = reader.readLine()) != null) {
            Map<String, String> data = new HashMap<>();

            String buf[] = parser.parseLine(line);
            for (String label : labelMap.keySet()) {
                int featureIdx = labelMap.get(label);

                data.put(label, buf[featureIdx]);
            }

            for (Vectorizer vectorizer : vectorizers) {
                double[] featureVec = new double[features.size()];

                Map<String, Double> result = vectorizer.vectorize(data);

                for (String feature : result.keySet()) {
                    double value = result.get(feature);

                    int outIdx = features.indexOf(feature);
                    featureVec[outIdx] = value;
                }

                DenseVector occurrenceVec = new DenseVector(featureVec);
                VectorWritable writable = new VectorWritable();
                writable.set(occurrenceVec);
                writer.append(new IntWritable(Integer.parseInt(buf[0])), writable);

                outCount++;
            }

        }

        writer.close();
        System.out.println("Done writing " + outCount + " vectors");

        // check file was written correctly
        System.out.println("Checking readability of " + outCount + " records");

        long checkCountInterval = 1000;
        long checkCount = 0;

        SequenceFile.Reader vectorReader = new SequenceFile.Reader(fs, new Path(vectorFile), conf);
        IntWritable key = new IntWritable();
        VectorWritable value = new VectorWritable();

        while (vectorReader.next(key, value)) {
            if ((checkCount % checkCountInterval) == 0) {
                //	Object outStr = key.toString()	+ " " +
                //			value.get();//.getDelegate().asFormatString();
            }
            checkCount++;
        }

        reader.close();
        vectorReader.close();
    }

}
