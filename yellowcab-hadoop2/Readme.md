
Qn 2
----
Filter all the records having RatecodeID as 4.

 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.hadoop.YellowCabJob2

Arguments : input/car.csv output/job2
 
Execute >> 
hadoop jar target/yellowcab-hadoop2-0.0.1.jar com.pgbde.hadoop.YellowCabJob2 input/input.txt output/job2/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-hadoop2-0.0.1.jar to /media/sf_VM/yellowcab-hadoop2-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup hadoop jar target/yellowcab-hadoop2-0.0.1.jar com.pgbde.hadoop.YellowCabJob2  /home/ec2-user/spark_assignment/input_dataset /home/ec2-user/spark_assignment/job2/ >> job2.txt
 
