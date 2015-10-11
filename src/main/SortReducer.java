package main;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long N = Integer.parseInt(context.getConfiguration().get("nodecount"));
		double threshold = (double) 5/N;
		double pagerank = Double.parseDouble(key.toString());
		
		long lesscount = 0;
		for (Text v: values){
			if (pagerank > threshold) {
				context.write(v, key);
			} else {
				//System.out.println("less than threshold: " + threshold);
				//context.write(v, key);
				lesscount++;
			}
		}
		System.out.println(lesscount);
	}
}
