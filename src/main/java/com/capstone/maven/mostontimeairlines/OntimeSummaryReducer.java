package com.capstone.maven.mostontimeairlines;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OntimeSummaryReducer extends
		Reducer<Text, BooleanWritable, Text, OntimeSummaryWritable> {
	private Map<String, OntimeSummaryWritable> summaryMap = new HashMap<String, OntimeSummaryWritable>();
	
	@Override
	public void reduce(Text key, Iterable<BooleanWritable> values, Context context) {
		summaryMap.putIfAbsent(key.toString(), new OntimeSummaryWritable());
		OntimeSummaryWritable summary = summaryMap.get(key.toString());
		for (BooleanWritable val : values) {
			if (val.get()) {
				summary.incrementOntime();
			} else {
				summary.incrementNotOntime();
			}
		}
		// summaryMap.put(key.toString(), summary);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		List<Entry<String, OntimeSummaryWritable>> list = new LinkedList<Entry<String, OntimeSummaryWritable>>(summaryMap.entrySet());
		Collections.sort(list, new Comparator<Entry<String, OntimeSummaryWritable>>() {
			public int compare(Entry<String, OntimeSummaryWritable> o1, Entry<String, OntimeSummaryWritable> o2) {
				return o2.getValue().getOntimeRate().compareTo(o1.getValue().getOntimeRate()); // reverse ordering
			}
		});
		
		final int topNCount = Integer.parseInt(context.getConfiguration().get("topNCount"));
		int i = 0;
		for (Iterator<Map.Entry<String, OntimeSummaryWritable>> it = list.iterator(); it.hasNext() && i < topNCount; i++) {
			Map.Entry<String, OntimeSummaryWritable> entry = (Map.Entry<String, OntimeSummaryWritable>) it.next();
			context.write(new Text(entry.getKey()), entry.getValue());
		}
	}
}
