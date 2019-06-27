package com.pgbde.spark;

import java.util.stream.StreamSupport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class YellowCabSparkJob3  {


	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);
		//SparkSession session = null;
		SparkConf conf =null;
		// For running on Eclipse- local mode
		if(args.length >2){
			System.out.println("Running in local mode");
			conf = new SparkConf().setAppName("SparkJob3").setMaster("local[*]");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").master("local[*]").getOrCreate();
		}else{
		// For running it on EC2 - Yarn client mode
			conf = new SparkConf().setAppName("SparkJob3");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").getOrCreate();
		}
		YellowCabSparkJob3 job = new YellowCabSparkJob3();
		job.execute(conf,args[0],args[1]);
		

		System.out.println("Finished process");
		
	}

	@SuppressWarnings("resource")
	private void execute(SparkConf sparkConf, String input, String output) {

		long start = System.currentTimeMillis();
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> plines = ctx.textFile(input, 1);
		
		JavaRDD<String[]> rowMapRdd = plines.map(line -> line.split(","));
		
		 
		//group by payment field
		
		JavaPairRDD<String,Long> paymentMap = rowMapRdd.mapToPair(record ->{
			if(record.length >Input.paymentColumn ) {
				return  new Tuple2<String, Long>(record[Input.paymentColumn],1L);
			}
			return new Tuple2<String, Long>("-1",0L);
		});
		//Use reduce by key to improve performance
		 JavaPairRDD<String, Long> groupByRDD = paymentMap.reduceByKey((x,y)-> x+y );
		
		//to sort - swap the RDD and sort by key (count) and swap back
		JavaPairRDD<Long,String>  tempSort = 
				groupByRDD.mapToPair(x ->x.swap());
		
		tempSort.cache();		
				
		JavaPairRDD<String,Long>  result = tempSort.sortByKey(false).mapToPair(x->x.swap());

		long total = System.currentTimeMillis() - start;
		System.out.println("Computation time taken: " + total / 60000 + " mins");

		System.out.println("Saving results...");
		result.saveAsTextFile(output);

		// closing spark context
		ctx.close();
		total = System.currentTimeMillis() - start;
		System.out.println("total time taken: " + total / 60000 + " mins");
		
	} 
 
}
