package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class YellowCabReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		long count = 0;
		for(LongWritable i : values){
			count = count+ i.get();
		}
		//System.out.println( key + "::"+ count);
		context.write(key, new LongWritable(count));
	}
}
