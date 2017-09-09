package com.capstone.maven.mostpopularairports;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private Map<String, Integer> countMap = new HashMap<String, Integer>();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {
		countMap.putIfAbsent(key.toString(), 0);
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		countMap.put(key.toString(), countMap.get(key.toString()) + sum);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(countMap.entrySet());
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue()); // reverse ordering
			}
		});
		
		final int topNCount = Integer.parseInt(context.getConfiguration().get("topNCount"));
		int i = 0;
		for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext() && i < topNCount; i++) {
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
	}
}
