package com.pgbde.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class YellowCabSparkJob1  {

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);
		//SparkSession session = null;
		SparkConf conf =null;
		// For running on Eclipse- local mode
		if(args.length >2){
			System.out.println("Running in local mode");
			conf = new SparkConf().setAppName("SparkJob1").setMaster("local[*]");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").master("local[*]").getOrCreate();
		}else{
		// For running it on EC2 - Yarn client mode
			conf = new SparkConf().setAppName("SparkJob1");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").getOrCreate();
		}
		YellowCabSparkJob1 job = new YellowCabSparkJob1();
		job.execute(conf,args[0],args[1]);
		

		System.out.println("Finished process");
		
	}

	@SuppressWarnings("resource")
	private void execute(SparkConf sparkConf, String input, String output) {

		System.out.println("input : "+ input + " : output : "+output);
		
		long start = System.currentTimeMillis();
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> plines = ctx.textFile(input, 1);
		
		JavaRDD<String[]> rowMapRdd = plines.map(line -> line.split(","));
		
		JavaRDD<String[]> filterRDD = rowMapRdd.filter( record -> {
			if(record.length > 5 && Input.VendorID.equals(record[0]) &&
						Input.tpep_pickup_datetime.equals(record[1])&&
						Input.tpep_dropoff_datetime.equals(record[2])&&
						Input.passenger_count.equals(record[3])&&
						Input.trip_distance.equals(record[4])){
				return true;
			}
			return false;
		}
		);
		
		JavaRDD<String> result = filterRDD.map(records ->{
			return String.join(",", records);
		});

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
