
Qn 1
----
Fetch the record having VendorID as '2' AND tpep_pickup_datetime as '2017-10-01 00:15:30' 
AND tpep_dropoff_datetime as '2017-10-01 00:25:11' 
AND passenger_count as '1' 
AND trip_distance as '2.17'
 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.spark.YellowCabSparkJob1

Arguments : input/car.csv output/job1
 
Execute >> 
spark jar target/yellowcab-spark1-0.0.1.jar com.pgbde.spark.YellowCabSparkJob1 input/input.txt output/job1/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-spark1-0.0.1.jar to /media/sf_VM/yellowcab-spark1-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup spark jar target/yellowcab-spark1-0.0.1.jar com.pgbde.spark.YellowCabSparkJob1  /home/ec2-user/spark_assignment/input_dataset /home/ec2-user/spark_assignment/job1/ >> job1.txt

nohup spark2-submit --class com.pgbde.spark.YellowCabSparkJob1 \
--master yarn --deploy-mode client \
yellowcab-spark1-0.0.1.jar \
/user/ec2-user/spark_assignment/yellow_tripdata/yellow_tripdata* \
/user/ec2-user/spark_assignment/output/spark/job1 >>spark-job1.txt
 
