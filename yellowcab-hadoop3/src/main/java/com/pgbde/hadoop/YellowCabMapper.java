package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowCabMapper  extends
		Mapper<LongWritable,Text, Text, LongWritable> {
	//2,2017-07-31 23:24:09,2017-07-31 23:52:14,1,5.81,1,N,164,74,1,22.5,0.5,0.5,4.76,0,0.3,28.56
	//2,2017-07-31 23:57:41,2017-08-01 00:03:28,1,1.23,1,N,41,166,1,6.5,0.5,0.5,1.56,0,0.3,9.36

	@Override
	protected void map(LongWritable key, Text record  ,
			Mapper<LongWritable,Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] split = record.toString().split("[,]");
		try{ 
			 
			System.out.println("record "+split[Input.paymentColumn]);
			context.write(new Text(split[Input.paymentColumn]),new LongWritable(1));
			 
		}catch(Exception e){
			System.out.print("YellowCabMapper : error: "+record.toString());
		}
		
	}
}