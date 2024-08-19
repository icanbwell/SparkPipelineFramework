# Partitioning
Spark is a distributed processing platform so it divides up tha data into partitions and sends different partitions to different executors to process and then it combines the results together.
This provides the power to process huge datasets on machines that have reasonably small CPU and memory.  

However it is important to use partitioning correctly.

So if we have a data frame with 1000 rows and the partition size is 100 then we will have 10 partitions with 100 rows each.

If we have 5 executors then Spark will send one partition each to each executor.  Once the executor finishes the partition then Spark will send the next unstarted partition to it.

Assuming each executor takes the same time to process a partition, this would mean each executor would process 2 partitions.  5 executors x 2 partitions = 10 partitions.

Within each partition, we create batches.  If the partition has 100 rows and our batch size is 20 then we end up with 5 batches per partition.  Each batch is processed as one operation e.g., all the ids in that batch will be sent to the FHIR server in one batch call.

# Why Partitioning is so important
If we don't partition correctly then we may have:
1. Executors sitting around idle while other executors are backlogged.
2. Executors running out of memory because the data processing they are doing takes too much memory
3. We are either sending too many data in each request to the external server so we get errors or we are sending too little so we are making too many calls to external server

# New FrameworkPartitioner
This is where the new FrameworkPractitioner v2 comes in (https://github.com/icanbwell/SparkPipelineFramework/blob/master/spark_pipeline_framework/transformers/framework_partitioner/v2/framework_partitioner.py).  It has this intelligence built in and all you have to do is drop this transformer in a pipeline, set the `calculate_automatically` parameter.  
The FrameworkPartitioner will then partition a DataFrame if needed based on the parameters provided.

# FrameworkPartitioner Algorithm
In general, we are trying to optimize the balance.  We want the make sure we have enough partitions
as number of executors, so executors are not sitting idle.  If we have 10 executors, then we want
at least 10 partitions.

But we are also trying to make sure we have the
partitions as big as the memory will allow, so we don't have the executors having to process multiple
partitions for a single task.  If we have 10 executors but 200 partitions, then each executor will
have to process 20 partitions, which is not efficient.

Lastly, we want to make sure that the partitions are not too big, so we don't run out of memory available
on the executors.

If the calculate_automatically parameter is set to True, then this code will automatically calculate
the number of partitions taking into account dataframe size, the number of executors
and memory available to each executor.

# Parameters
You can view all the parameters accepted by FrameworkPartitioner here: https://github.com/icanbwell/SparkPipelineFramework/blob/master/spark_pipeline_framework/transformers/framework_partitioner/v2/framework_partitioner.py

# Caveats
By default, one of the things the FrameworkPartitioner uses is the count of rows in the source dataframe, the size of the first input row and size of output row.  
These can be expensive operations depending on your processing so you can specify these as parameters to avoid FrameworkPartitioner calculating these.

Currently the FrameworkPartitioner does not specifically address skew (where some partitions are much bigger than others), however the process of re-partitioning can help balance the partitions in many cases.


