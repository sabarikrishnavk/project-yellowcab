package com.pgbde.spark;

public class Input {
	public static final String 	VendorID = "2" ;
	public static final String  tpep_pickup_datetime = "2017-10-01 00:15:30"; 
	public static final String  tpep_dropoff_datetime = "2017-10-01 00:25:11"; 
	public static final String  passenger_count = "1"; 
	public static final String  trip_distance = "2.17";
	
	public static final String filterString =  VendorID+","+ 
									tpep_pickup_datetime +","+
									tpep_dropoff_datetime +","+
									passenger_count +","+
									trip_distance +","; 
}
