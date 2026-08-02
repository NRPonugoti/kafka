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
 
