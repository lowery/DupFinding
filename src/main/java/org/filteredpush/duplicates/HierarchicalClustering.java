package org.filteredpush.duplicates;

import au.com.bytecode.opencsv.CSVParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HierarchicalClustering {
    private final CSVParser parser;
    private final Map<String, Integer> labelMap;

    Map<String, List<String>> eventDateClusters = new HashMap<>();

    public HierarchicalClustering(Config config) throws IOException {
        OccurrenceData occurrenceData = new OccurrenceData(config);
        labelMap = occurrenceData.getLabelMap();

        parser = occurrenceData.getMetadata().getParser();
        BufferedReader reader = occurrenceData.getReader();

        String line;

        while ((line = reader.readLine()) != null) {
            String[] data = parser.parseLine(line);

            String eventDate = data[labelMap.get("eventDate")];
            //String occurrenceId = data[labelMap.get(config.getRecordID())];

            //  Skip rows with empty event date or value of 0000-00-00
            if (!(eventDate.isEmpty() || eventDate.equals("0000-00-00"))) {

                if (!eventDateClusters.containsKey(eventDate)) {
                    eventDateClusters.put(eventDate, new LinkedList<String>());
                }

                List<String> cluster = eventDateClusters.get(eventDate);
                cluster.add(line);

            }

        }

        String clustersFile = config.getWorkingDir() + "eventDateClusters.csv";
        PrintWriter writer = new PrintWriter(clustersFile);

        // Header from label map
        String[] headerArray = new String[labelMap.size()];
        for (String key : labelMap.keySet()) {
            headerArray[labelMap.get(key)] = key;
        }

        for (int i = 0; i < headerArray.length; i++) {
            writer.append(headerArray[i]);
            if (i < headerArray.length-1) {
                writer.append(",");
            }
        }

        writer.append("\n");

        for (String key : eventDateClusters.keySet()) {
            List<String> cluster = eventDateClusters.get(key);

            // experiment with the scientific names
            Map<String, List<String>> scientificNameClusters = cluster("scientificName", cluster);

            writeClusters(scientificNameClusters, writer);
        }

        writer.close();
    }

    private void writeClusters(Map<String, List<String>> clusters, PrintWriter writer) {
        for (String key : clusters.keySet()) {
            List<String> cluster = clusters.get(key);

            if (cluster.size() > 1) {
                for (String record : cluster) {
                    writer.println(record);
                }

                writer.println("cluster");
            }
        }
    }

    private Map<String, List<String>> cluster(String feature, List<String> cluster) throws IOException {
        Map<String, List<String>> clusters = new HashMap<>();

        for (String line : cluster) {
            String[] data = parser.parseLine(line);
            String key = data[labelMap.get(feature)];

            if (!clusters.containsKey(key)) {
                clusters.put(key, new LinkedList<String>());
            }

            List<String> newCluster = clusters.get(key);
            newCluster.add(line);
        }

        return clusters;
    }
}
