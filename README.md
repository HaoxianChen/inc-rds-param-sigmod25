#### 1.incremental_rds_parameter_updates_release

    Incremental rule discovery algorithm under parameter updates developed in this study

#### 2.Algorithm Entry Function

rule_dig/inc_rule_dig.go 

    -- rule_dig.IncDigRulesDistribute()  // Entry point for incremental rule discovery

        -- batchMiner()                    // batch

        -- incMinerSupport()               // update support, confidence remains unchanged

        -- incMinerConfidence()            // update confidence, support remains unchanged

        -- incMinerIndicator()             // update both support and confidence simultaneously

#### 3. Running the Incremental Algorithm

##### 3.1 Required Components

| Component Name | Version  |
| -------------- | -------- |
| PostgreSQL     | 9.6.24   |
| etcd           | 3.5.6    |
| Golang         | 1.19     |

##### 3.2 Initialization SQL Script

	After installing the PostgreSQL database, run the init_sql/init_sql.sql script to initialize the database tables.

##### 3.3 Deploying API and Manager Images

Related images and configurations are located in the images directory

```
# Create directories: api-etc, api-log, manager-etc, manager-log
mkdir api-etc api-log manager-etc manager-log

# Copy api-api.yaml to the api-etc directory
cp api-api.yaml api-etc/

# Copy manager.yaml and rock_config.yml to the manager-etc directory
cp manager.yaml rock_config.yml manager-etc/
```

Modify configuration files

```
# Edit the api-api.yaml file
Modify the Etcd.Hosts entry, filling in the actual etcd IP and port

```
# Edit the manager.yaml file
1. Modify the Etcd.Hosts entry, filling in the actual etcd IP and port
2. Modify the Host, Port, User, Password, and Database under Pg, filling in the actual values
3. Modify the RockEtcd.Endpoints entry, filling in the actual etcd IP and port
4. Modify the RockCluster.Ssh.Hosts entry, specifying the IP of the running node, filling in the actual value
5. Modify the RockCluster.Ssh.User entry, the server node username (all nodes must have the same username), filling in the actual value
6. Modify the RockCluster.Ssh.Password entry, the server node password (all nodes must have the same password), filling in the actual value
```

```
# Edit the rock_config.yml file
1. Modify the host, port, user, password, and dbname under pg_config, filling in the actual values
2. Modify the etcd_config.endpoints entry, filling in the actual etcd IP and port
```

Load the API and Manager images

```
docker load -i api_image.tar
docker images
docker load -i manager_image.tar
docker images
```

Deploy manager and API

```
# Deploy manager
docker run -d --privileged=true --volume=./manager-log:/app/logs --volume=./manager-etc:/app/etc --net=host manager_image:v1.0
# Deploy API
docker run -d --privileged=true --volume=./api-log:/app/logs --volume=./api-etc:/app/etc --net=host api_image:v1.0

##### 3.4 Deploying the Storage Image

The related image is located in the images directory

```
# Load the image
docker load -i storage_image.tar
# Push the image to the local image repository
Execute docker tag to rename the image according to the actual local image repository address
Execute docker push to push the newly tagged image to your local image repository
```

Deploy storage using the following command, where:
- **ip** is the IP address of the deployment server
- **userId** is any positive integer (e.g., 123, 196, etc.)
- **image_path** is the storage image path in your local image repository

```
# Deployment command
curl -X POST http://{ip}:8889/create-task \
   -H "Content-Type: application/json" \
   -d '{
      "userId": {userId},
      "taskType": 5,
      "deployType": 0,
      "dockerImage": "{image_path}"
     }'

If you want to stop the container and redeploy, execute the following command, where **ip** and **taskId** should be filled in based on the actual deployment.

```
# Stop command
Check the storage container NAMES on the server: 196-5-593-4793, where 196 is the userId, 5 is the taskType, 593 is the taskId, and 4793 is the identifier.
curl -X POST http://{ip}:8889/close-task \
   -H "Content-Type: application/json" \
   -d '{
      "taskId": {taskId},
      "closeType": 100
     }'
```

##### 3.5 Deploying the reeminer Image

Package the reeminer image

```
# [image_name] is the image name, [tag] is the tag label, and the Dockerfile is located in the project's root directory
docker build -t [image_name]:[tag] -f Dockerfile .
# Push the image to the local image repository
docker push [image_name]:[tag]
```

Deploy reeminer using the following command, where:
- **ip** is the IP address of the deployment server
- **userId** must be the same as the one used for storage (otherwise, the container will fail to start)
- **image_path** is the reeminer image path in your local image repository

```
# Deployment command
curl -X POST http://{ip}:8889/create-task \
   -H "Content-Type: application/json" \
   -d '{
      "userId": {userId},
      "taskType": 1,
      "deployType": 0,
      "dockerImage": "{image_path}"
     }'
```

If you want to stop the container and redeploy, execute the following command, where **ip** and **taskId** should be filled in based on the actual deployment.

```
# Stop command
Check the reeminer container NAMES on the server: 196-1-594-4603, where 196 is the userId, 1 is the taskType, 594 is the taskId, and 4603 is the identifier.
curl -X POST http://{ip}:8889/close-task \
   -H "Content-Type: application/json" \
   -d '{
      "taskId": {taskId},
      "closeType": 100
     }'
```

##### 3.6 Executing the Task

1. **Import the dataset into the PostgreSQL database**

2. **Modify configuration**

   The scripts are located in `inc_rule_dig/auto_test/auto_test.py` and `auto_test_vary_d_large.py`.

   Modify the **ip** and **port** values in the main method of the script. Use the following command to obtain the reeminer service's **ip** and **port**:

```
# Execute on the deployed etcd server
etcdctl get --prefix rock/{userId}/task/1
# Example output:
rock/196/task/1/602/1/196-1-602-4467
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6000","IP":"172.165.14.50","Port":19124,"GrpcPort":20000,"TcpPort":30000}
rock/196/task/1/602/2/196-1-602-1257
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6070","IP":"172.165.14.47","Port":19124,"GrpcPort":20000,"TcpPort":30000}
rock/196/task/1/602/2/196-1-602-2128
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6052","IP":"172.165.14.45","Port":19124,"GrpcPort":20000,"TcpPort":30000}
...

# Choose any node from the output
Example: IP: 172.165.14.50, Port: 19124
```

Modify the `dataSources` configuration in `inc_rule_dig/auto_test/*.json`. The reeminer service will load database data based on this configuration:

```
# dataSources configuration
  "dataSources": [
    {
      "database": "postgres",
      "host": "127.0.0.1",
      "port": 5432,
      "databaseName": "xxxx",
      "tableName": "inc_rds.hospital",
      "user": "xxxx",
      "password": "xxxx"
    }
  ],
```

Modify **host, port, databaseName, user,** and **password** according to your actual PostgreSQL configuration.

Modify the `inc_rule_dig/auto_test/*.xlsx` file to set the actual experiment parameters. **Ensure that the dataset name column contains the actual table name in PostgreSQL (e.g., `inc_rds.hospital`).**

3. **Run the Python scripts directly**

