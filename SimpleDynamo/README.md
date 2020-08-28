# Amazon Dynamo
A simplified version of Amazon Dynamo styled distributed key-value storage which provides linearizability and availability at the same time under failures.
Used quorum based replication with object versioning to achieve linearizability and availability at the sametime. Partition and replication is done exactly the way Amazon Dynamo does.
