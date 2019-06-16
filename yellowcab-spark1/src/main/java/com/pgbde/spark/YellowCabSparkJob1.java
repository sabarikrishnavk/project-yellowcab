package com.pgbde.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class YellowCabSparkJob1  {

	 

	public static void main(String[] args) throws Exception {
		SparkSession session = null;
		//SparkConf conf =null;
		// For running on Eclipse- local mode
		if(args.length >2){
			//sessi = new SparkConf().setAppName("my_spark_App").setMaster();
			session = SparkSession.builder().appName("YellowCabSparkJob1").master("local[*]").getOrCreate();
		}else{
		// For running it on EC2 - Yarn client mode
			//conf = new SparkConf().setAppName("my_spark_App");
			session = SparkSession.builder().appName("YellowCabSparkJob1").getOrCreate();
		}
		YellowCabSparkJob1 job = new YellowCabSparkJob1();
		job.execute(session,args[0],args[1]);
		
		
		
	}

	String[] columnNames = new String[] {
			"VendorID",
			"tpep_pickup_datetime","tpep_dropoff_datetime",
			"passenger_count","trip_distance",
			"RatecodeID",
			"fwd_flag","PULocationID","DOLocationID",
			"payment_type",
			"fare_amount","extra","mta_tax","tip_amount","tolls_amount",
			"improvement_surcharge","total_amount" 
		};
	private void execute(SparkSession session, String input, String output) {
		DataFrameReader reader = session.read();
		Dataset<Row> rows = reader.option("inferSchema", "true")
				.csv(input).toDF(columnNames);
		//rows.select("VendorID","tpep_pickup_datetime").show();
		rows.select("VendorID","tpep_pickup_datetime").write().format("csv").save(output);
	}

	/*public void execute(SparkConf conf, String input, String output) {
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(input);
		 
		
		data.collect().forEach(record -> {
			System.out.println(record);
		});
	}*/
}