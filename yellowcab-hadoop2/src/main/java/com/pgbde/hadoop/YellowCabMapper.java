package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowCabMapper  extends
		Mapper<LongWritable,Text, Text, NullWritable> {
	//2,2017-07-31 23:24:09,2017-07-31 23:52:14,1,5.81,1,N,164,74,1,22.5,0.5,0.5,4.76,0,0.3,28.56
	//2,2017-07-31 23:57:41,2017-08-01 00:03:28,1,1.23,1,N,41,166,1,6.5,0.5,0.5,1.56,0,0.3,9.36

	@Override
	protected void map(LongWritable key, Text record  ,
			Mapper<LongWritable,Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] split = record.toString().split("[,]");
		try{ 
			if(Input.RatecodeID.equals(split[5])){
				//System.out.println("record "+record);
				context.write(record, NullWritable.get());
			}
		}catch(Exception e){
			System.out.print("YellowCabMapper : error: "+record.toString());
		}
		
	}
}