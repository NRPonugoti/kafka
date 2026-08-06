# kafka
event-driven-architecture
The Reality of Most Microservices
====================================
On paper, many organizations claim to have microservices.

Architecture diagrams often look clean:

Order Service
Payment Service
Inventory Service
Shipping Service
Notification Service

Each service has a clear responsibility.

However, in reality, many of these systems behave like distributed monoliths.
Why?

Because the services communicate through synchronous REST calls.
Although these are separate services, they are tightly coupled at runtime.

The customer receives a successful response only if every service is:

Available
Fast
Error-free

-----------------------------------------------------------

Problems with Synchronous Communication
=========================================
Example: Placing an Order

When a customer places an order:

Order Service receives the request.
Calls Product Service for pricing.
Calls Payment Service.
Payment Service calls Fraud Service.
Order Service calls Inventory Service.
Calls Shipping Service.
Shipping Service calls Notification Service.

To the customer, this appears to be one request.

Behind the scenes, it is a long chain of dependent service calls.

-------------------------------------------------------------------------------------
Problem 1: Slow Services
Suppose the Fraud Service becomes slow.

Suppose the Fraud Service becomes slow.

Then:

Payment Service waits.
Order Service waits.
Customer waits.

The delay propagates through the entire chain.

Even though only one service is slow, the whole application feels slow.

Latency accumulates.

--------------------------------------------------------------------------------------------
Problem 2: Service Failure


Now imagine the Fraud Service is unavailable.

Perhaps:

Network failure
Service crash
Deployment issue

The Payment Service fails.

The Order Service cannot continue.

The customer's order fails with a 500 Internal Server Error.

Now the customer must retry.Some customers retry.

Some simply leave.

A single internal failure becomes a poor customer experience.
-----------------------------------------------------
Problem 3: Adding New Features
Suppose the business wants to add a Recommendation Service.

For every order, the system should recommend related products.

In a synchronous architecture:

Order Service must call Recommendation Service.
Existing code must be modified.
The complete workflow must be tested again.
Another dependency is introduced.

Every new feature makes the dependency chain longer.

Longer chains mean:
Higher risk
More testing
More failures

Small changes become dangerous.

The Bigger Picture

In synchronous microservices:

Services depend on each other.
One slow service slows everyone.
One failed service may fail the entire request.
Every new feature increases coupling.
All services must be available simultaneously.

Although the architecture is called "microservices," it behaves like one tightly coupled application.










Event Driven MicroServices 
-----------------------------
what if the services do not have to call each other directly 

instead of sending requests and waiting for response , these services can publish events 
and move on , other services can react to these events independently whenever they are ready 
this is code idea behind event driven architecture 
communication becomes asynchronous and non-blocking 
services become loosely coupled 
the system no longer depends on every service being available at the same time 

In event driven systems , the order service no longer coordinate the workflow when customer places the order 
, it simply publishes an event ,order placed event and immediatly returnes 202 accepted response and its job done 
the order service does not need to know which service will process the payment , which service will update the inventory etc

each of these services subscribes to the order event and handle it own responsibility independently 
so complex orchestration disappears , communication become simple 

publish once and react independently , that is how the event driven architecture removes the need for complex synchronous coordination 

In an event driven system , a slow service does not block the entire flow 
in an event driven systems , a failure does not immediatly break the entire operation 
the order placed event is stored durably in the messaging system if the payment service fails while processing it ,
the order placed event is not lost , the customer does not have to retry , the service can retry processing later 
when it recovers , failures are handled as part of the work flow 

In event driven systems , services are decoupled from each other ,the order service does not know about the order services 
that means we can easily introduce a new service 

In a event driven systems , services do not have to be available  at the same time 
when the order placed event is published , its stored in the broker , other application can process it whenever they are available 


By shifiting from synchronous communication to event-driven  communication , we fundamently change how the systems behave 
dependencies become looser , failuere become manageable , scaling become pratical and adding new capability become safer 

its different way to desing systems for the real world 










# uanching the kafka Container 

docker compose fire then run it 

# docker compose up 
kafka server is start , whenever kafka server starts ,it will printing all the properties it uses 

#docker exec -it kafka bash 
#ls -l
to check path 
$ echo $PATH , /opt/kafka/bin is not part of the path , because of this reason it cannot execute like this 
so when every you try to execute any of these commands you have to always add ./
$ ./kafka-topics.sh 

/opt/kafka$ ls -l 
$ cd config/ 
$ ls -l 
 broker.properties 
 consumer.properties 
 controller.properties 
 producer.properties 
 
 # Kafka Core Fundamentals :
 ============================
 
Event : anything that "happend" 
Also known as 
        * Record 
		* Message 


Kafka is an event streaming platform , it can capture any event whenevery it occures in your application 
then it stores those event safely in some place intenrally , so that they can be delivered to other application 
it can be delivery the messages in real time for procesing 

Kafka is opensource and distributed 
itmostly developed in java , some components in Scala 

# how does kafka store the messages? 

Kafka has something called Topic ,its way of collecting and organizing data within  a kafka cluster 



<img width="1166" height="493" alt="image" src="https://github.com/user-attachments/assets/94aeb597-1b1e-45d5-a4bb-5c4ad6bbc863" />


let's assume that this is our kafka server and we have some topics created 

whenever an User places an order , this application writes that information as event in this kafka topic , this could be order event topic 
Then we have another application develoled using Python , it could be a payment service , this application consumes messages and charges payment for the user 

Any Application producess messages is called a Producer , similarly , any applicaiton which consumes the messages is called a Consumer 



what will happen if this single Kafka node crashes due to out of memory ?
what will happen to our applicaiton ? how can they communicate ? what will happen to all our orders ?

Kafka Cluster ?

Running a single Kafka server is Okay for local development or lower environment like DEV, QA 
but we don not do that for production environment 
we will be running multiple kafka server together in clustomer mode 
each box represnet one Kafka Node , we call the whole setup a kafka cluster 

<img width="1519" height="751" alt="image" src="https://github.com/user-attachments/assets/d0218877-888d-41dd-9dee-85ea55ce81e2" />

Advantages : 

High availability  : any single node failure will not affect our application 
Horizontal Scalability : Our application are going to talk to the kafka server , send messages to the Kafka server 
how many request can a single Kafka server can handle at the time , so we have to run multiple Kafka instances to distributed the Load 



# Kafka Node Roles :

Two Roles 

1. Broker  : The Producer and the consumer appication will be talking to a broker , we can have multiple brokers in a cluster 
     * Stores data 
	 * Handles read and write requests 
2. Controller : Responsible for the managing the cluster becuase in a large cluster , then there are multiple machines working togheter 
   someone has to act like a manager and guide those node 
   
   at any given time , we will be having only one active controller in a cluster and controller does not handle the client communication 
   
   * Manages the kafka cluster 
   * Not involved in the Client Communication 
   * At any given time , A Cluster has only one Active Contrller 
   * Manages Brokers CoOrdination 
   * One Node act  like a Controller 
  
  If we can only one Controller , what if the controller die ? 
  again , it is going to be a single point of failure , Actually No , All Orther Nodes , they can elect Another Node as a new Controller 
  This is how we can assign the roles to a kafka Node 
  Property 
  
  # Broker 
              process.roles=broker 
             #Controller-only Node 
             Process.roles=controller 
             #broker + controller eligible node 
             process.roles=broker, controller 


<img width="1797" height="751" alt="image" src="https://github.com/user-attachments/assets/4205793e-ec2b-4f50-98b7-b0ccdcfa0517" />

 Process.roles=controller 
We have cluster with 100 Nodes , we make 3 node as a controller But One Node act as a Controller and Other 2 Controller are Standby  If Controller Node die , Standby Node will become a Controller 

  Inside Kafka Container :
  /opt/kafka/config$ grep '^process.role' broker.properties 
  process.role=broker 
  /opt/kafka/config$ grep '^process.role' controller.properties 
  process.role=controller 
 
 /opt/kafka/config$ grep '^process.role' server.properties 
  process.role=broker , controller 
  
  /opt/kafka/config$ ../bin/kafka-server-start.sh  broker.properties 
  
  so by default in this docker container , it uses server.properties  



# Kafka Cluster 
<img width="1374" height="728" alt="image" src="https://github.com/user-attachments/assets/7cbe4c27-c7dd-49c7-be24-745f4c885cd9" />



  Let's imagine , we want to create a topic called Order event , The Controller it is a boss 
  so controller will find one broker and it will ask this broker that hey broker you own the Order event topic 
  you handle the read and write request and this broker will happily accept that 
  so when ever producer application produce the order events so this broker will write that in the machine when the consumer 
  application asks for the messages ,the broker will delivery those  message 
  What if this  broker mission dies ? 
  
  this is why then the controller finds the broker , it will also identify few other nodes to be the backup
  so the data will be replicated to other instances as well 


  # Summerize 
  
   - Single kafka Node is okay for development purpose But its not good for Production environment where we need high availability and harizontal scalability 
   - So we will be running multiple kafka servers in the cluster mode , we developers we simply call them Kafka server or kafka Node 
	         But they can play specific roles in the cluster 
   - Brokers to handles read and write request 
   - Cotroller to manage the cluster Operations 
   - Small cluster in the standalone kakfa instance like docker container , it will have both broker / controller roles 
   - At any given time we will be having only one controller if the controller goes down for some reason , 
	       ther will be another controller eligible node , it will be ready to take over immediately 
   - similarly when the leader broker dies for some reason there might be another follower broker , it will be ready to take over immediately
      So there will not be any single point of failure and our application will not be interupted 
	 
# BootStrap Server :
     If i run a Kafka cluster with hundreads or thousands of nodes , how can i talk to a specfic broker ?
	 
	 so My Application if its going to produce order events , how can it directly or correctly go and talk to this specific broker ?
	 How do I know ?
	 Brokers are like family they are know each other very well 
	 As long as if your application can connect to this (one ) Broker then that's it the very next seconds , your application will come to know 
	 about the entire cluster 
	  This broker will give all the detials to this machine that this broker has order events , thos broker has payment event etc 
	  so it will provide the entire cluster meta data to this application 
	  Actually we dont have to worry about writing the code to get that cluster infomration and managing them and all in our application 
	  its already handled as part of the Kafka official client library which we will be adding in our application 
	  
	  
Note : So even If you have thousand of servers in your cluster , as Long as you know the one Single server connectivity detials 
        your application can work just fine without any issues we call that bootstrap server 


		<img width="1715" height="802" alt="image" src="https://github.com/user-attachments/assets/a1148e91-aabb-41e6-9bdd-222522568242" />


My Application is trying to connect to the cluster using this IP Address(Node) But if this node is down , what will happen ?

it is a Problem , In those cases , you can identify a few more machines , like set of machines , Anything can act like a bootstrap server 
So you can identify a few more machines and you can provide them as a list in your application.properties 
So as long as youa application can talk to one of these servers , Then it will work just fine 




# Demo Kafka Topic 

$ docker exec -it kafka bash 
/opt/kafka/bin$ ls 
/opt/kafka/bin$ ./kafka-topics.sh 
--bootstrap-server<String: server to connect to>  Required 
--create                                          create a topic 
--delete                                          delete a topic 
--list                                            List all available topics 
--topic <String: topic>                           the topic to create 

# create a topic 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payment-events 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic shipping-events 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --list 

#more details about the topic 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --describe  --topic order-events 

# Delete tpic 
/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic order-events 


# Demo: Console Producer 

kafka console producer :  learning , testig 

/opt/kafka/bin$ ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic dem-topic 
/opt/kafka/bin$ ./kafka-console-producer.sh  --ENTER 
 --bootstrap-server<String: server to connect to>  Required the server to connect to the borker list strng in the form HOST1:PORT1 ,HOST2:PORT2
 --topic<String : topic>REQUIRED: the topic name to produce message to 
/opt/kafka/bin$  ./kafka-console-producer --bootstrap-server localhost:9092 --topic demo-topic  --Enter 
> hello 
>1

# Demo: Console Consumer 

/opt/kafka/bin$ ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-tpic 

the consumers by default they will be consuming only the new messages 
we can adjust this behaviour 
i wnat to see all message latst and old messages 
#opt/kakfa/bin$ ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-project --from-beginning  
consumer see all message 


# Console Producer TimeOut Configuration :
/opt/kafka/bin$ ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-topic 


/opt/kafka/bin$ ./kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic demo-topic 

Producer sending message but consumer consume the message with some delay and getting batch messages sometimes 
no kafka server slow , this console producer behaviour , overriding one property 

Console Producer 
/opt/kafka/bin$ ./kafka-console-producer --bootstrap-server localhost:9092 --topic demo-topic --timeout 0 
 
timeout by default is 1 second  like 1000ms 

 show timeout options --default 1 seconds 
#opt/kafka/bin$ ./kafka-console-producer.sh 
--timeout <Long :timeout_ms>        `linger.ms` in producer configs 

console producer keeps the messages in a queue then it delivers them in batches 


# linger.ms vs batch.size 

<img width="1082" height="530" alt="image" src="https://github.com/user-attachments/assets/f5b79c7d-2b71-4e57-bd57-c3c5ab48ffcf" />




# Consumer -Push or pull ?
whenever we send the messages to the kafka server ,does the kafka server push messages to the consumer or the consumer pulls 
the messages from the kafka server ? 

so kafka server will not push the messages to the consumer , The consumer has to pull the messages from the kafka topic or broker 

The Connection b/w the kafka broker and the consumer is the persistent TCP Connection so why can this Kafka server not push the messages using the connection ? 

for kafka , each and every messages has to be delivered to the consumer and it expects some kind of an acknowledgement from consumer that , yes i saw that messages if we skip the whole acknowledgement step ,what will happen here is 
producer is producing super fast ,10,000 messages per second , kafka broker might be pushing all the messages to the consumer but what if the consumer is not able to keep up with this producer speed ? so now we will be losing the messages 
your are seeing the problem 

so for kafka , each and every messages has to be processed safely and reliably because of this reason , it will not push the message instead , it will ask the consumer to ask for the messages , only then it will delivery 

### consumer Properties 
                       max.poll.records: 500
Producer sends 10000 messages to kafka server but consumer takes 500 messages as per max.poll.records 
<img width="1700" height="794" alt="image" src="https://github.com/user-attachments/assets/9ae5a0bc-da9a-4d3e-9c05-480c227e2729" />




# How kafka store messages internally ?

Kafka stores and it transports everything as bytes , it does not understand your mesage
kafka job accept the messages from the producer and delivery the messages to the consumer 
Serialization and deserialization its done by the individual applications , not broker 

Producer side  configure how to serialize the messages similarly the consumer side we have to configure how to deserialize the messages 
Kafka client library comes with some basic serializer and deserializer 

Can use jackson library to serialize our object into JSON then JSON can be converted into bytes array that is how we will be sending 
Spring framework will be doing all this heavy lifting for us 

### Console Producer / Console Consumer 
    - Tools for learning and testing 
	- Treat messages as String by default 
	- Under the hood : 
	        Producer ==> String ----> byte[] 
			Consumer ==> byte[] ----> String 
			
		
		
# Log Retention 

kafka store data on disk and consumer can read that data later 

How long kafka keep the data ? 
depends on the log retention policy 
server.properties files 
be default , kafka keeps data for 168 hours which is seven days 
log.retention.hours=168 
log.retention.bytes



# Offset Fundamentals  - Offset in kafka topics 

whenever the messages are sent to a topic , kafka will be storing the messages in in the order it receives 
it delivery the messages only in the order it received 
so it assign one unique number we call that Offset like array Index it starts from Zero 

OffSet max value = Long.MAX_VALUE 

<img width="1234" height="591" alt="image" src="https://github.com/user-attachments/assets/689e5c2d-3c99-4165-80d0-4fbde7a9bcb8" />


<img width="1225" height="593" alt="image" src="https://github.com/user-attachments/assets/fd7e2db8-c4e1-4ad5-9f71-328a0a08a9af" />




<img width="1191" height="498" alt="image" src="https://github.com/user-attachments/assets/e806ead5-9ca8-4501-977b-dd815e087d1e" />


In some cases , you might want to know when the messages were produced so timestamp of the messages 
so if you want , we can also print that 

<img width="1320" height="330" alt="image" src="https://github.com/user-attachments/assets/3aa1d287-c97a-4da2-acf4-040b7fe54695" />



# Multiple Consumers 

 One Producer and 2 Consumers 

 One Producer produces the message and 2 Consumers are consumes the messages 

 Order-service microsservices , it keeps on sending the messages to a kafka topic called order events 
 there are two other micro services , inventory service and payment service , Both of them are interested in consuming 
 these events because it has to process inventory and payment for the order 
 but in real life , I will not be running one single instance in the production 
 I would be running multiple instances of inventory services , multiple instances of payment serivce 
 so the problem here is all the instances will be receving the same order event so we will end up doing redundant processing 

 what we really want is only one instance should be receiving that order event then all order instances should not be receiving the same event 
 so this is exactly what want 

<img width="1225" height="592" alt="image" src="https://github.com/user-attachments/assets/e05c03b3-8eee-4642-9eb5-b71c7e05ee5a" />

 Lets discuss How Kafka Solves this problem , Kafka has a concept of Consumer Group 

 <img width="1862" height="908" alt="image" src="https://github.com/user-attachments/assets/db305988-dc76-4521-b5ea-86f87a471736" />


<img width="1794" height="868" alt="image" src="https://github.com/user-attachments/assets/76771410-02a3-43ff-b99b-8dc435c21d96" />


when we have multiple consumers in a Consumer Group only one consumer gets the messages so we do not do redundant processing 
When we have multiple consumers from different group ,both of them get the messages 

when we have multiple consumers in a consumer group , we could have probably expected The messages should have been distributed b/w these two consumers , however  it does not seem to be behaving like this , Only one consumer gets all the messages . default behavior 
### List all the consumer groups 
  ./kafka-consumer-group.sh --bootstrap-server localhost:9092 --list 


<img width="1342" height="440" alt="image" src="https://github.com/user-attachments/assets/2f45312e-416d-41e0-9735-becc6d831339" />


<img width="1855" height="920" alt="image" src="https://github.com/user-attachments/assets/010eae8a-b18b-4eae-ac66-71d8df507812" />


# Message Ordering 

kafka is an event streaming platform and message ordering is very importanet for kafka 
and in the same order , it will be delivering the messages to the consumer 

if I have a topics and if i have thousands of events then I cannot horizontally scale 
I have to have only one consumer to process all the events but actually No , Kafka gives a solution for that 
That is where the partition concept comes in 

<img width="1249" height="536" alt="image" src="https://github.com/user-attachments/assets/62cd4f0b-87d4-4cda-ba4b-30632e49eafd" />

# Topics / Partitions 
Topic is logical abstraction 
Partitions are physical storage units that where the data is stored 
so we can say a topic is divided into multiple partitions 


when we create a topic we have to mention how many partitions we want 
./kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 2


<img width="1257" height="591" alt="image" src="https://github.com/user-attachments/assets/247a2833-458f-4d53-9b4f-84fea625124b" />

Message what ever we produce that can have a key 
partition = hash(key) % number_of_partitions 

	How are we solving the scalability issues ? 
since we have multiple partitions and the order is guaranteed within the accounts , kafka can happily assign the whole partition to this consumer , now we have parallel processing 

offset belongs to the partitions 

There is a very good chance that one partition might be having more messages compared to another partition 

### who is calculating this portition 
is this done by the kafka server ? actually No 
it is done by client library , In our application , we will be producing messages , events along with key then we will be submitting the messages or providing the messages to the client library to send to kafka so that time the client ibrary using the key then using this code , it will be finding the partition then it will be submitting the information to the kafka server saying this message goes to this partition and all 

<img width="1878" height="859" alt="image" src="https://github.com/user-attachments/assets/fe6188dd-9cf7-4585-b0e1-557e8910d086" />

# Demo: Multi-Partition Topic 

<img width="1591" height="890" alt="image" src="https://github.com/user-attachments/assets/713aef83-62eb-4cbe-94a3-cfc9d6b2bb4d" />

This is actually correct for a topic with one partition when you create a topic with only one partition 
so there will be one node that could be the leader for the topic 


<img width="1257" height="645" alt="image" src="https://github.com/user-attachments/assets/ef25bc91-70e2-46ae-894c-5e9e327d48ac" />

there will be one node that could be the leader for the topic or leader for the partition 
there could be another node follower for the partition 

when we create topic with multiple partitions , it is not like all the partitions will be part of the single node 
the controller might distribute these partition across different broker node 
so if you create one topic with two partition , one leader for the partition 0 and another leader for partition 1 

The Partition can be distributed across different nodes 
.opt/kafka/config$grep '^node.id' server.properties 
node.id=1 
in the cluster , each and every single node will be given one unique ID 
# Demo : Consumer Group with 2 Partitions 
<img width="1805" height="807" alt="image" src="https://github.com/user-attachments/assets/a02feca2-806e-4475-93ca-e815f4b50d4a" />



# Partition Rebalancing 
 I am having a topic with 2 partition and one consumer is starting with consumer group: payment-service and i am sending an messages and those messages received the single consumer 

 after sometime ,One more consumer now is joining from the same payment-service group when this consumer joins ,kafka will do something called partition rebalancing 
 what it means is that when this consumer joins , kafka can detect this , now it can see that i am having this topic with 2 partitions , I have assigned both partitions to this consumer , Now I am seeing the one new consumer from the consumer group 
 so i can assign one partition to this consumer and another partition to this consumer 

<img width="1829" height="827" alt="image" src="https://github.com/user-attachments/assets/417792c9-6789-4b5b-b33d-cf9b32778161" />

Whenever consumers join a group , whenever consumers leave a group , kafka will be doing the partition rebalancing 

<img width="1762" height="881" alt="image" src="https://github.com/user-attachments/assets/0d2879a7-e0c6-4d91-94c3-8704fa1f01b3" />


# Modifying Partitions Count 
./kafka-topics.sh  --bootstrap-server localhost:9092 --topic order-events --alter  --partitions 4 

<img width="1227" height="578" alt="image" src="https://github.com/user-attachments/assets/7e6c7f39-43ac-4587-be97-227583304da0" />


<img width="1026" height="386" alt="image" src="https://github.com/user-attachments/assets/66daf728-5ce5-4179-a971-0b370f42fc8d" />



# Kafka Summary 

<img width="1859" height="920" alt="image" src="https://github.com/user-attachments/assets/e5dd55ef-0429-489f-85d9-ce7622d8b196" />


we normally do not run one single kafka server in Production environment 
we will be running a group of kafka servers as a cluster 
Each kafka Node will start with some roles either broker or controller or broker, controller 
if Node has broker role , it will handle client request , read and write 
If Node has a Controller role , it is eligible to play the controller role 
