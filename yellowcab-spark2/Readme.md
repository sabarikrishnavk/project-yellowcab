
Qn 2
----
Filter all the records having RatecodeID as 4.

 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.spark.YellowCabSparkJob2

Arguments : input/car.csv output/job2
 
Execute >> 
spark jar target/yellowcab-spark2-0.0.1.jar com.pgbde.spark.YellowCabSparkJob2 input/input.txt output/job2/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-spark2-0.0.1.jar to /media/sf_VM/yellowcab-spark2-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup spark jar target/yellowcab-spark2-0.0.1.jar com.pgbde.spark.YellowCabSparkJob2  /home/ec2-user/spark_assignment/input_dataset /home/ec2-user/spark_assignment/job2/ >> job2.txt
 
nohup spark2-submit --class com.pgbde.spark.YellowCabSparkJob2 \
--master yarn --deploy-mode client \
yellowcab-spark2-0.0.1.jar \
/user/ec2-user/spark_assignment/yellow_tripdata/yellow_tripdata* \
/user/ec2-user/spark_assignment/output/spark/job2 >>spark-job2.txt

spark-submit --class com.pgbde.spark.YellowCabSparkJob2 --master spark://127.0.0.1:7077 --deploy-mode client /home/workspace/project-yellowcab/yellowcab-spark2/target/yellowcab-spark2-0.0.1.jar /home/workspace/input/ /home/workspace/output/spark/job2
 
 