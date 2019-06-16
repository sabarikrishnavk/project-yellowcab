package com.pgbde.spark;

import org.apache.spark.SparkConf;


public class YellowCabJob1  {

	 

	public static void main(String[] args) throws Exception {
		// For running on Eclipse- local mode
		SparkConf conf = new SparkConf().setAppName("my_spark_App").setMaster("local[*]");
		// For running it on EC2 - Yarn client mode
		//SparkConf conf = new SparkConf().setAppName("my_spark_App");
	}
}
