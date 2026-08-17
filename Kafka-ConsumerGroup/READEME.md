# Scaling Consumers with Consumer Groups

we are going to observe when we run multiple consumers from a consumer group , how partitions are distributed among the consumers 
how we could scale message processing using consumer groups , what happens when new consumers join a group existing consumers leave a group 

<img width="1698" height="820" alt="image" src="https://github.com/user-attachments/assets/d54ba0ab-988c-4f38-9481-9c6c6a142649" />

<img width="1804" height="842" alt="image" src="https://github.com/user-attachments/assets/f7df0764-73a8-4280-ab5b-d18d2c34bbe9" />

Step1 : Create a topic with 3 partitions 
Step 2: Start one consumer , kafka assign 3 partitions to this consumer 
Step3 : Start one more consumer and imagine  join another consumer , now kafka doing partition rebalancing 
         Kakfa assign first consumer with partition 2 
		 Kafka assign 2nd consumer with partition 0,1 
Step4 : again start one more consumer then kafka do rebalancing 
           Kakfa assign first consumer with partition2
		   Kafka assign 2nd consumer with partition 1 
		   Kafak assign 3rd consumer with partition 0 

Step5 : Kill First Consumer then kafka do the partition rebalancing and assign the partition 0 to cnsumer 2 : partition 0,1 



<img width="1245" height="610" alt="image" src="https://github.com/user-attachments/assets/3952d7be-829c-4f3a-ae27-05f7ca1d5803" />
