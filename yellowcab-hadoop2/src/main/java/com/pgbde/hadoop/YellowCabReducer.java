package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class YellowCabReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

			//System.out.println("value "+key);
			context.write(key, NullWritable.get());
	}
}
