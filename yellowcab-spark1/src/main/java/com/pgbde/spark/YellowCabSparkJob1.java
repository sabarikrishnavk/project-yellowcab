package com.pgbde.spark;

import java.io.StringReader;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import au.com.bytecode.opencsv.CSVReader;


public class YellowCabSparkJob1  {

 

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.ERROR);
		//SparkSession session = null;
		SparkConf conf =null;
		// For running on Eclipse- local mode
		if(args.length >2){
			System.out.println("Running in local mode");
			conf = new SparkConf().setAppName("my_spark_App").setMaster("local[*]");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").master("local[*]").getOrCreate();
		}else{
		// For running it on EC2 - Yarn client mode
			conf = new SparkConf().setAppName("my_spark_App");
			//session = SparkSession.builder().appName("YellowCabSparkJob1").getOrCreate();
		}
		YellowCabSparkJob1 job = new YellowCabSparkJob1();
		job.execute(conf,args[0],args[1]);
		

		System.out.println("Finished process");
		
	}

	private void execute(SparkConf sparkConf, String input, String output) {

		long start = System.currentTimeMillis();
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaPairRDD<String,String> csvData = ctx.wholeTextFiles(input);
		
		JavaRDD<String[]> rows = csvData.flatMap(new ParseLine());
		
		
		
		JavaRDD<String[]> result = rows.filter( record -> {
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


		long total = System.currentTimeMillis() - start;
		System.out.println("Computation time taken: " + total / 60000 + " mins");

		System.out.println("Saving results...");
		result.saveAsTextFile(output);

		// closing spark context
		ctx.close();
		total = System.currentTimeMillis() - start;
		System.out.println("total time taken: " + total / 60000 + " mins");
		
		
		
	}
	
	public static class ParseLine implements FlatMapFunction<Tuple2<String,String>,String[] >{

		/**
		 * 
		 */
		private static final long serialVersionUID = 5277342667942925905L;

		@Override
		public Iterator<String[]> call(Tuple2<String, String> file)
				throws Exception { 
			return extracted(file).readAll().iterator();
		}

		private CSVReader extracted(Tuple2<String, String> file) {
			return new CSVReader(new StringReader(file._2) );
		}
		
	}
 
}
