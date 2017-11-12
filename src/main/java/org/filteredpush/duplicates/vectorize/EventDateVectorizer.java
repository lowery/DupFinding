package org.filteredpush.duplicates.vectorize;

import org.filteredpush.duplicates.VectorizationAlgorithm;
import org.filteredpush.duplicates.VectorizeGBIFOccurrences;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EventDateVectorizer implements Vectorizer {
    private final String[] features = { "eventDate" };

    @Override
    public String[] listFeatures() {
        return features;
    }

    @Override
    public Map<String, Double> vectorize(Map<String, String> record) {
        Map<String, Double> featureVec = new HashMap<>();

        for (String feature : features) {
            String data = record.get(feature);
            double value = 0.0;

            try {

                DateTime dt = DateTime.parse(data);

                value = VectorizeGBIFOccurrences.vectorize(dt);
            } catch (Exception e) {
                System.err.println("Error vectorizing " + feature + "=" + data);
                e.printStackTrace();
            }

            featureVec.put(feature, value);

        }

        return featureVec;
    }
}
