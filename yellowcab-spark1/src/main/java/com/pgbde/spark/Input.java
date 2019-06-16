package com.pgbde.spark;

public class Input {
	public static final String 	VendorID = "2" ;
	public static final String  tpep_pickup_datetime =  "2017-10-01 00:15:30"; 
	public static final String  tpep_dropoff_datetime = "2017-10-01 00:25:11"; 
	public static final String  passenger_count = "1"; 
	public static final String  trip_distance =  "2.17";
	
	//Sample entry : 2,2017-07-31 23:21:45,2017-07-31 23:30:11,2,1.17
	/***
	 * 
Fetch the record having VendorID as '2' 
AND tpep_pickup_datetime as '2017-10-01 00:15:30' 
AND tpep_dropoff_datetime as '2017-10-01 00:25:11' 
AND passenger_count as '1' 
AND trip_distance as '2.17'
	 */
	public static final String filterString =  "VendorID='"+VendorID+"' AND "+ 
			"tpep_pickup_datetime='"+tpep_pickup_datetime +"' AND "+
			"tpep_dropoff_datetime='"+tpep_dropoff_datetime +"' AND "+
			"passenger_count='"+passenger_count +"' AND "+
			"trip_distance='"+trip_distance +"'  "; 
}
