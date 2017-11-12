package org.filteredpush.duplicates;

import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevensteinDistance;
import org.joda.time.DateTime;

/**
 * @author Robert A. Morris
 * 
 */
public class VectorizeGBIFOccurrences {

	// TODO Auto-generated constructor stub

	public VectorizeGBIFOccurrences() {
		
	}

	public static double vectorize(String data, VectorizationAlgorithm algorithm) {

		switch (algorithm) {
		case JARO_WINKLER:
			JaroWinklerDistance jwdist = new JaroWinklerDistance();
			if ("NULL".equals(data) ||"".equals(data) || "s.n.".equals(data)) data = "NULL";
			return (double) jwdist.getDistance("abcdefghijklmnopqrstuvwxyz.,;|ABCDEFGHIJKLMNOPQRSTUVWXYZ", data);
		case LEVENSTEIN:
			LevensteinDistance ldist = new LevensteinDistance();
			if ("NULL".equals(data) ||"".equals(data) || "s.n.".equals(data)) data = "NULL";
			return (double) ldist.getDistance("abcdefghijklmnopqrstuvwxyz.,;|1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ", data);
		case DATE_TIME:
			return "".equals(data) ? 0.0 : DateToMillis
					.normalizedMillisFromEpoch(data);
		case LAT_LONG:
			break;
		default:
			break;
		}
		return 0.0;
	}
	public static double vectorize(DateTime data){
		return DateToMillis.normalizedMillisFromEpoch(data);
		
	}
}
