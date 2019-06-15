
Qn 3
----
Group By all the records based on payment type and 
find the count for each group. 
Sort the payment types in ascending order of their count
 
To run the program in eclipse
--

Run As Java program and update the program arguments in Run As configuration


Java program : com.pgbde.hadoop.YellowCabJob3

Arguments : input/car.csv output/job3
 
Execute >> 
hadoop jar target/yellowcab-hadoop3-0.0.1.jar com.pgbde.hadoop.YellowCabJob3 input/input.txt output/job3/
 
To run in AWS
--
Copy the jar via winscp to cloudera ec2 instance
Connect via putty

Copy target/yellowcab-hadoop3-0.0.1.jar to /media/sf_VM/yellowcab-hadoop3-0.0.1.jar
Execute >>
sudo rm -rf /yarn/nm/usercache/ec2-user

nohup hadoop jar target/yellowcab-hadoop3-0.0.1.jar com.pgbde.hadoop.YellowCabJob3  /home/ec2-user/spark_assignment/input_dataset /home/ec2-user/spark_assignment/job3/ >> job3.txt
 
