package org.filteredpush.duplicates;

import java.io.*;
import java.util.Map;

public class OccurrenceData {
    private BufferedReader reader;
    private GbifMetadata meta;

    private Map<String, Integer> labelMap;

    public OccurrenceData(Config config) throws IOException {
        // Load data
        String dataset = "occurrences" + (config.isUseSmall() ? ".small" : "") + ".csv"; // add postfix to use .small or .med test set
        String occurrenceFileName = config.getWorkingDir() + dataset;
        System.err.println("occurrenceFileName=" + occurrenceFileName);

        //InputStream in = this.getClass().getClassLoader().getResourceAsStream(inputString);
        FileInputStream in = new FileInputStream(occurrenceFileName);
        char quoteChar = config.getQuoteChar();
        char separator = config.getOccurrenceSeparator();

        reader = new BufferedReader(new InputStreamReader(in));
        meta = new GbifMetadata(reader, separator, quoteChar);

        // init label map
        labelMap = meta.getlabelMap(); // ADVANCES READER BY FIRST LINE, which has headers

        System.out.println("labelMap size: " + labelMap.size());
        System.out.println("labelMap = " + labelMap);
        System.err.println("131: " + config.getWorkingDir() + " " + config.getRecordID());

        int idIdx = labelMap.get(config.getRecordID());
        assert (idIdx == 0); // it should be the first column
    }

    public BufferedReader getReader() {
        return reader;
    }

    public GbifMetadata getMetadata() {
        return meta;
    }

    public Map<String, Integer> getLabelMap() {
        return labelMap;
    }
}
