
Qn 1
----
Fetch the record having VendorID as '2' AND tpep_pickup_datetime as '2017-10-01 00:15:30' 
AND tpep_dropoff_datetime as '2017-10-01 00:25:11' 
AND passenger_count as '1' 
AND trip_distance as '2.17'
 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.hadoop.YellowCabJob1

Arguments : input/car.csv output/job1
 
Execute >> 
hadoop jar target/yellowcab-hadoop1-0.0.1.jar com.pgbde.hadoop.YellowCabJob1 input/input.txt output/job1/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-hadoop1-0.0.1.jar to /media/sf_VM/yellowcab-hadoop1-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup hadoop jar target/yellowcab-hadoop1-0.0.1.jar com.pgbde.hadoop.YellowCabJob1  /home/ec2-user/spark_assignment/input_dataset /home/ec2-user/spark_assignment/job1/ >> job1.txt
 
nohup hadoop jar yellowcab-hadoop1-0.0.1.jar \
com.pgbde.hadoop.YellowCabJob1 \
/user/ec2-user/spark_assignment/yellow_tripdata/yellow_tripdata* \
/user/ec2-user/spark_assignment/output/mapred/job1 >>mapred-job1.txt
