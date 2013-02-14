# Aggregate

Aggregate is an application that consumes events and aggregates them.  It uses
cassandra as the backend and rather than queues i've opted to use disruptor to
manage message passing between the publisher of events and the aggregators that
do the counting.

The majority of the work is forwarded to the cassandra instance.

## the CounterSpace
allow the use of counters where row keys can be:
- UUID
- String
- Long (timestamp)

and the column name is optional

## Test scenario
### Direct line to Cassandra 
Workers talk directly to Cassandra rather than having one consumer piping 
messages through Disruptor


Start of CounterWorker Cassandra counter test
5656 TPS with 1 Process loop Handler/s

6860 TPS with 2 Process loop Handler/s
6860 TPS with 2 Process loop Handler/s

5783 TPS with 3 Process loop Handler/s
5784 TPS with 3 Process loop Handler/s
5786 TPS with 3 Process loop Handler/s

4743 TPS with 4 Process loop Handler/s
4753 TPS with 4 Process loop Handler/s
4742 TPS with 4 Process loop Handler/s
4744 TPS with 4 Process loop Handler/s

3842 TPS with 5 Process loop Handler/s
3839 TPS with 5 Process loop Handler/s
3845 TPS with 5 Process loop Handler/s
3840 TPS with 5 Process loop Handler/s
3840 TPS with 5 Process loop Handler/s

3136 TPS with 6 Process loop Handler/s
3132 TPS with 6 Process loop Handler/s
3135 TPS with 6 Process loop Handler/s
3138 TPS with 6 Process loop Handler/s
3133 TPS with 6 Process loop Handler/s
3133 TPS with 6 Process loop Handler/s

### Single Publisher -> Primitive Aggregation with PrimitiveAggregateHandlers
a Single Publisher publishes a series of primitives to a Singleton ringbuffer,
through a worker pool that simply imcrements counters in cassandra.
The key to the counter is the a String composed of the Primitives and a
timestamp.

3183  TPS with 1 Worker/s
6436  TPS with 2 Worker/s
10073 TPS with 3 Worker/s
14064 TPS with 4 Worker/s
25442 TPS with 5 Worker/s
34107 TPS with 6 Worker/s

a single publisher for a Singleton RingBuffer performs better than multiple
Publishers to a Singleton RingBuffer.

### Multi Publisher With independent RingBuffers with PrimitiveAggregateHandlers
this Test is to see if we can increase the throughout by increasing the number
of publishers and also mapping a RingBuffer per publisher.


### Multi Publisher -> Primitive Aggregation with PrimitiveAggregateHandlers
contention with multiple publishers to a Singleton RingBuffer indicates the
need for testing with multiple ringBuffers; one ringBuffer per Publisher. 

### In Memory Primitive Aggregation with PrimitiveFunctionAggregateHandlers 
pass a primitive through a worker pool that steps through a sequence Barrier
to escalate through a sequence of handling functions
1) publish to ringbuffer
2) aggregate over time
3) publish aggregation upon a given interval to cassandra
### Simple String Aggregation with ByteArrayAggregateHandlers
pass a String (encoded as a UTF8 byte array) and increment counters in
cassandra
### In Memory String Aggregation with ByteArrayFunctionAggregateHandlers
pass a Byte Array through a worker pool that steps through a sequence Barrier
to escalate through a sequence of handling functions
1) publish to ringbuffer
2) aggregate over time
3) publish aggregation upon a given interval to cassandra
