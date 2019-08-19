
# Rdbms To Dynamodb With Aws Glue

Migrating data from RDBMS to DynamoDB using AWS Glue.

## Prerequisite

Clone repository awslabs/emr-dynamodb-connector using the following command.
```git clone https://github.com/awslabs/emr-dynamodb-connector.git```

Once cloning is complete build the repository using the following command.
```mvn clean install```

Upload the jar files from emr-dynamodb-connector/emr-dynamodb-hadoop/target/emr-dynamodb-hadoop-x.y.z-SNAPSHOT.jar and emr-dynamodb-connector/emr-dynamodb-hive/target/emr-dynamodb-hive-x.y.z-SNAPSHOT-jar-with-dependencies.jar to s3://<bucket name>/jars

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.
