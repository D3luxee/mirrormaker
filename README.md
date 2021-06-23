# mirrormaker

A simple mirrormaker written in go, supporting compression and different partitioners for mirroring.


## Features
* Compression of messages (gzip,lz4,snappy,none)
* Partitioning in different ways:
  * hash (it will read the partition key of the source message and partition it again)
  * keepPartition (it will write the message to the same partition on the target topic as it was read from the source topic)
  * random (just a random partitioner)
  * modulo (SourcePartiton % NumPartitionsOfTargetTopic) this works good if you want to replicate from many to less partitions. If the source topic has less or the same number of partitions this will work like keepPartition.
