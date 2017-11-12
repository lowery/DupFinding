package org.filteredpush.duplicates.vectorize;

import java.util.Map;

public interface Vectorizer {
    public String[] listFeatures();
    public Map<String, Double> vectorize(Map<String, String> record);
}
