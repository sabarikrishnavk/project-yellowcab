
Qn 3
----
Group By all the records based on payment type and 
find the count for each group. 
Sort the payment types in ascending order of their count
 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.spark.YellowCabSparkJob3

Arguments : input/car.csv output/job3
 
Execute >> 
spark jar target/yellowcab-spark3-0.0.1.jar com.pgbde.spark.YellowCabSparkJob3 input/input.txt output/job3/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-spark3-0.0.1.jar to /media/sf_VM/yellowcab-spark3-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup spark jar target/yellowcab-spark3-0.0.1.jar com.pgbde.spark.YellowCabSparkJob3  /home/ec2-user/spark_assignment/input_dataset
 /home/ec2-user/spark_assignment/spark/job3/ >> job3.txt
 
nohup spark2-submit --class com.pgbde.spark.YellowCabSparkJob3 \
--master yarn --deploy-mode client \
yellowcab-spark3-0.0.1.jar \
/user/ec2-user/spark_assignment/yellow_tripdata/yellow_tripdata* \
/user/ec2-user/spark_assignment/output/spark/job3 >> spark-job3.txt
 
 spark-submit --class com.pgbde.spark.YellowCabSparkJob3 --master  spark://localhost:7077 --deploy-mode client /home/workspace/project-yellowcab/yellowcab-spark3/target/yellowcab-spark3-0.0.1.jar /home/workspace/input/ /home/workspace/output/spark/job3
 
