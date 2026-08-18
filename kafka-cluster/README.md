
<img width="1815" height="911" alt="image" src="https://github.com/user-attachments/assets/b90c0056-d81f-4eb3-8506-c39d05237c4f" />
<img width="1747" height="790" alt="image" src="https://github.com/user-attachments/assets/7e35f743-b996-4e02-9ee8-8aa89e7e1351" />

<img width="1874" height="856" alt="image" src="https://github.com/user-attachments/assets/62c7b4d6-10b3-4cd6-b9aa-b33c2ec2eb61" />

Kafka Cluster, Scalability & High Availability — Revision Notes
1. Main Goal

The main goal of setting up a Kafka cluster is to understand how Kafka achieves:

High Availability (HA)
Horizontal Scalability
Fault Tolerance
Capacity

In production, we normally don't run a single Kafka server. Instead, we run multiple Kafka nodes together as a cluster.

Important: Simply adding more Kafka brokers does not automatically provide scalability and high availability. Kafka uses partitions and replication to achieve these.

2. Stateless Application vs Stateful Kafka
Stateless Application — Example: Spring Boot Microservice

A typical Spring Boot microservice is stateless.

For example:

              Load Balancer
                    |
       +------------+------------+
       |            |            |
    App-1         App-2        App-3
       |            |            |
       +------------+------------+

If traffic increases:

1 instance → 10 instances → 100 instances

You simply:

Start more instances.
Put them behind a load balancer.
Distribute requests.

Every instance can generally handle any request.

Kafka is Stateful

Kafka is different because it stores data on disk.

Kafka brokers contain:

Topics
Partitions
Messages/events
Replicas

Therefore, you cannot simply send a request to any random broker and expect it to handle a particular partition.

3. Kafka Cluster

A Kafka cluster consists of multiple Kafka servers/nodes.

                 Kafka Cluster
        +-----------------------------+
        |                             |
     Broker 1      Broker 2      Broker 3
        |             |             |
     Data          Data          Data
        |             |             |
        +-------------+-------------+

Each Kafka node can have different roles.

The important roles are:

Broker
Controller
4. Broker Role

The broker handles client requests.

For example:

Producer → Kafka Broker
Consumer → Kafka Broker

A broker handles:

Client read requests
Client write requests
Storing partition data
Serving partition data to consumers

Most nodes in a Kafka cluster will typically have the broker role.

5. Controller Role

The controller is responsible for managing the Kafka cluster.

When configuring a Kafka server, you can specify:

process.roles=broker

or:

process.roles=controller

or:

process.roles=broker,controller
Important Point

Having the controller role does not mean that the node is automatically the active controller.

It means that the node is eligible to participate in controller election.

At any given time:

Only one active controller exists in the Kafka cluster.

If the active controller fails:

Controller 1
     ↓
   FAILED
     ↓
Controller Election
     ↓
Controller 2 becomes active

This provides fault tolerance for cluster management.

6. What Does a Controller Do?

The controller manages important cluster-level operations, such as:

Partition leadership
Broker membership
Partition assignments
Controller election
Cluster metadata

For example, when a topic is created, the controller determines which brokers should handle its partitions.

7. Topic = Collection of Partitions

A Kafka topic is an abstract logical concept.

Physically, a topic consists of partitions.

For example:

Topic: product-view-events


        +----------------+
        |    Topic       |
        | product-view   |
        +----------------+
                |
        +-------+-------+
        |               |
   Partition 0     Partition 1

Partitions are extremely important because they provide parallelism and scalability.

8. Problem with One Partition

Suppose we create:

Topic: product-view-events
Partitions: 1

The controller selects one broker as the leader:

Broker 1
   |
   +---- Partition 0

Even if we have:

Broker 1
Broker 2
Broker 3
Broker 4
Broker 5
...
Broker 100

only one broker is responsible for that partition if there is no additional replication.

Therefore, having 100 brokers doesn't automatically mean the topic can use all 100 brokers for parallel processing.

9. Real-World Example

Imagine an e-commerce website such as Amazon.

Suppose we have:

100 Product Service Instances

Each service generates:

Product View Event

For example:

Customer views iPhone
       ↓
Product Service
       ↓
Product View Event
       ↓
Kafka

If the topic has only one partition:

Product Service 1 ──┐
Product Service 2 ──┤
Product Service 3 ──┤
Product Service 4 ──┤
       ...           ├──→ Broker 1
Product Service 100─┘      |
                         Partition 0

All producers ultimately target the same partition.

Therefore, one partition can become a bottleneck.

10. Partitions Are the Fundamental Unit of Parallelism

This is one of the most important Kafka concepts.

Partitions are the fundamental unit of parallelism in Kafka.

If you want more scalability, you generally need more partitions.

For example:

1 Partition
     ↓
Limited parallelism


2 Partitions
     ↓
More parallelism


10 Partitions
     ↓
Much more parallelism
11. Multiple Partitions

Suppose we create:

Topic: product-view-events
Partitions: 2

The controller can assign the partitions to different brokers:

Broker 1
   |
Partition 0


Broker 2
   |
Partition 1

Now the workload can be distributed across multiple brokers.

Therefore:

Partitions provide scalability and parallelism.

12. Consumer Parallelism

Partitions also determine consumer parallelism.

Suppose:

Topic
   |
Partition 0

and we have:

Consumer Group
   |
Consumer 1
Consumer 2
Consumer 3

Only one consumer can actively consume that single partition at a time.

With two partitions:

Partition 0 → Consumer 1
Partition 1 → Consumer 2

Now two consumers can process messages in parallel.

Important Rule

Within a consumer group:

Maximum active consumers = number of partitions

For example:

Partitions	Consumers	Active Consumers
1	3	1
2	3	2
3	3	3
5	3	3
10	3	3

Extra consumers beyond the number of partitions will remain idle.

13. Scalability vs Availability

This is a very important distinction.

Adding partitions helps with:

Scalability

But partitions alone don't necessarily provide:

High Availability

Consider:

Broker 1
   |
Partition 0

What happens if Broker 1 fails?

Broker 1 ❌
   |
Partition 0 ❌

The partition becomes unavailable.

Therefore:

Partitions provide scalability, but replication provides high availability.

14. Replication Factor

Kafka provides high availability using replication.

When creating a topic, we can specify:

replication factor

The replication factor tells Kafka:

How many copies of each partition should exist in the cluster.

15. Replication Factor = 1

Suppose:

Partitions = 2
Replication Factor = 1

We might have:

Broker 1
   |
Partition 0


Broker 2
   |
Partition 1

There is only one copy of each partition.

If Broker 1 fails:

Broker 1 ❌
   |
Partition 0 ❌

There is no other copy available.

Therefore:

Replication factor 1 = No redundancy

16. Replication Factor = 3

Now consider:

Partitions = 2
Replication Factor = 3

Kafka creates three copies of each partition.

For Partition 0:

Broker 1
   |
Partition 0
   |
 Leader

and:

Broker 2
   |
Partition 0 Replica


Broker 3
   |
Partition 0 Replica

So:

Partition 0
   |
   +── Leader
   +── Replica
   +── Replica

That's 3 total copies.

17. Leader and Followers

For each partition, Kafka has a leader replica.

Other replicas are followers.

Example:

Partition 0


Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

The leader handles client operations for that partition, while followers replicate the data.

Conceptually:

Producer
    |
    ↓
Leader
    |
    +------→ Follower
    |
    +------→ Follower

The data is continuously replicated.

18. What Happens When a Broker Fails?

Suppose:

Partition 0


Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

Now Broker 1 fails:

Broker 1 ❌

Kafka can elect/promote one of the available replicas as the new leader.

For example:

Broker 2 → New Leader
Broker 3 → Follower

Therefore, the partition can continue to be available.

This is how replication contributes to high availability.

19. Important Kafka Formula

Remember this:

Brokers       → Capacity
Partitions    → Scalability / Parallelism
Replication   → High Availability

This is probably the most important summary from this section.

20. Complete Example

Suppose we create:

Topic: product-view-events
Partitions: 2
Replication Factor: 3

We could have:

                  Kafka Cluster


        Broker 1       Broker 2       Broker 3
           |               |               |
           |               |               |
       P0 Leader       P0 Replica      P1 Replica
           |               |               |
           |               |               |
       P1 Leader       P1 Replica      P0 Replica

Conceptually:

Partition 0
   ├── Broker 1 → Leader
   ├── Broker 2 → Replica
   └── Broker 3 → Replica


Partition 1
   ├── Broker 2 → Leader
   ├── Broker 1 → Replica
   └── Broker 3 → Replica

Now we have:

Scalability

Two partitions can be processed in parallel.

High Availability

Each partition has three copies.

Fault Tolerance

If one broker fails, another replica can potentially become leader.

21. Key Concepts to Remember
Broker

A Kafka server/node that handles client read/write requests and stores partition data.

Controller

Manages the Kafka cluster and participates in cluster-management operations.

Active Controller

Only one active controller exists at a given time.

Topic

Logical grouping of messages/events.

Partition

Physical/logical unit where Kafka stores and distributes records.

Leader

The active replica responsible for client operations for a partition.

Follower

A replica that follows/replicates the leader's data.

Replication Factor

Number of total copies of each partition.

22. Exam/Interview Questions
Q1. Does adding more brokers automatically provide scalability?

No.

You need appropriate partitioning to distribute workload across brokers.

Q2. What provides scalability in Kafka?

Partitions.

Partitions allow Kafka to process data in parallel.

Q3. What provides high availability?

Replication.

Multiple replicas of a partition allow another replica to take over if a broker fails.

Q4. What happens with replication factor 1?

There is only one copy of each partition.

If its broker fails, the partition becomes unavailable.

Q5. What does replication factor 3 mean?

There are three total copies of each partition:

1 Leader + 2 Followers = 3 copies
Q6. Can multiple consumers in the same consumer group consume one partition simultaneously?

No.

One partition can be assigned to only one consumer within a consumer group at a time.

Q7. What is the maximum number of active consumers in a consumer group?

Approximately:

Number of active consumers ≤ Number of partitions
23. Final Revision Cheat Sheet
Kafka Cluster
     |
     +── Brokers
     |     └── Handle client requests + store data
     |
     +── Controllers
           └── Manage cluster
Topic
  |
  +── Partition 0
  +── Partition 1
  +── Partition 2
Partition
  |
  +── Leader
  +── Replica
  +── Replica
Remember:
BROKERS
   ↓
Capacity


PARTITIONS
   ↓
Parallelism + Scalability


REPLICATION
   ↓
Redundancy + High Availability

One-line memory trick:

Brokers give you capacity, partitions give you scalability, and replication gives you availability.



# Cluster Configuration Properties 
<img width="1275" height="466" alt="image" src="https://github.com/user-attachments/assets/74f9fd51-6410-4273-962f-b7c7f3744572" />


<img width="1737" height="827" alt="image" src="https://github.com/user-attachments/assets/57ce2907-e7aa-4c67-b53f-a4f4ba84493d" />

<img width="1553" height="673" alt="image" src="https://github.com/user-attachments/assets/6a472198-5545-43f2-bb12-3ccffcd11bb6" />


kafka is statefull application , it store data on disk 

A Kafka cluster does not give the high availability and horizantal scalability automatically 
the multiple brokers in the kafka cluster provide only the capacity 
only partitions provides the scalability and the replication provides the availability 
we were able to setup kafka cluster with a docker containers and we were able to create a topic with multiple partitions 
and a replication factor so that kafka could elect multiple leaders for each partitions 
we were able to see the followers for our topic partitions to replicate the data for high availability 


we also noticed that how the bootstrap server was able to give the other server information to our application 
once application is connected to the cluster via bootstrapserver then even when bootstrap server goes down
our application just fine 
