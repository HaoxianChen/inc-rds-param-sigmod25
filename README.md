#### 1.incremental_rds_parameter_updates_release

​    在本研究中开发的参数更新下的增量规则发现算法

#### 2.算法入口函数

rule_dig/inc_rule_dig.go 

​	-- rule_dig.IncDigRulesDistribute()  // 增量规则发现的入口函数

​		-- batchMiner()                    // batch

​        -- incMinerSupport()		  // support更新，confidence不变

​		-- incMinerConfidence()	// confidence更新，support不变

​		-- incMinerIndicator()		// support、confidence同时更新

#### 3.运行增量算法

##### 3.1需要安装的组件

| 组件名称   | 版本   |
| ---------- | ------ |
| PostgreSQL | 9.6.24 |
| etcd       | 3.5.6  |
| Golang     | 1.19   |

##### 3.2初始化sql脚本

​	安装好PostgreSQL数据库后，运行init_sql/init_sql.sql脚本，初始化数据库表。

##### 3.3部署api、manager镜像

​	相关镜像、配置在images目录下

```
# 创建api-etc、api-log、manager-etc、manager-log目录
mkdir api-etc api-log manager-etc manager-log

# 把api-api.yaml放到api-etc目录下
cp api-api.yaml api-etc/

# 把manager.yaml、rock_config.yml放到manager-etc目录下
cp manager.yaml rock_config.yml manager-etc/
```

​	修改配置文件

```
# 编辑api-api.yaml文件
修改Etcd.Hosts项，按实际填写etcd的ip和port
```

```
# 编辑manager.yaml文件
1.修改Etcd.Hosts项，按实际填写etcd的ip和port
2.修改Pg下的Host、Port、User、Password、Database，按实际填写
3.修改RockEtcd.Ednpoints项，按实际填写etcd的ip和port
4.修改RockCluster.Ssh.Hosts项，此处指定运行节点的ip，按实际填写
5.修改RockCluster.Ssh.User项，服务器节点用户名，各个节点用户名需保持一致，按实际填写
6.修改RockCluster.Ssh.Password项，服务器节点密码，各个节点密码需保持一致，按实际填写
```

```
# 编辑rock_config.yml文件
1.修改pg_config下的host、port、user、password、dbname，按实际填写
2.修改etcd_config.endpoints项，按实际填写etcd的ip和port
```

​	加载api、manager的镜像

```
docker load -i api_image.tar
docker images
docker load -i manager_image.tar
docker images
```

​	部署manager、api

```
# 部署manager
docker run -d --privileged=true --volume=./manager-log:/app/logs --volume=./manager-etc:/app/etc --net=host manager_image:v1.0
# 部署api
docker run -d --privileged=true --volume=./api-log:/app/logs --volume=./api-etc:/app/etc --net=host api_image:v1.0
```

##### 3.4部署storage镜像

​	相关镜像在images目录下

```
# 加载镜像
docker load -i storage_image.tar
# 把镜像推到本地镜像仓库
执行docker tag，按实际本地镜像仓库地址重新命令镜像
执行docker push，把新命名的镜像推送到自己的本地镜像仓库
```

​	按以下命令部署storage，其中：ip为部署的服务器的ip地址，userId任意正整数(如：123、196等)，image_path本地镜像仓库中storage的镜像路径。

```
# 部署命令
curl -X POST http://{ip}:8889/create-task \
   -H "Content-Type: application/json" \
   -d '{
      "userId": {userId},
      "taskType": 5,
      "deployType": 0,
      "dockerImage": "{image_path}"
     }'
```

​	如果想要停止容器并重新部署，按以下命令执行，其中：ip、taskId根据实际部署情况填写。

```
# 停止命令
服务器上查看storage的容器NAMES:196-5-593-4793,196是userId,5是taskType,593是taskId,4793是编号,
curl -X POST http://{ip}:8889/close-task \
   -H "Content-Type: application/json" \
   -d '{
      "taskId": {taskId},
      "closeType": 100
     }'
```

##### 3.5部署reeminer镜像

​	打包reeminer镜像

```
# image_name镜像名称，tag镜像tag标签，Dockerfile工程的根目录下的Dockerfile文件
docker build -t [image_name]:[tag] -f Dockerfile .
# 推送镜像到本地镜像仓库
docker push [image_name]:[tag] 
```

​	按以下命令部署reeminer，其中：ip为部署的服务器的ip地址，userId必须保持和storage的userId一致，否则容器启动失败，image_path本地镜像仓库中reeminer的镜像路径。

```
# 部署命令
curl -X POST http://{ip}:8889/create-task \
   -H "Content-Type: application/json" \
   -d '{
      "userId": {userId},
      "taskType": 1,
      "deployType": 0,
      "dockerImage": "{image_path}"
     }'
```

 	如果想要停止容器并重新部署，按以下命令执行，其中：ip、taskId根据实际部署情况填写。

```
# 停止命令
服务器上查看reeMiner的容器NAMES:196-1-594-4603,196是userId,1是taskType,594是taskId,4603是编号,
curl -X POST http://{ip}:8889/close-task \
   -H "Content-Type: application/json" \
   -d '{
      "taskId": {taskId},
      "closeType": 100
     }'
```

##### 3.6执行任务

(1)导入数据集到PostgreSQL数据库

(2)修改配置

​	脚本目录inc_rule_dig/auto_test/auto_test.py、auto_test_vary_d_large.py

​	需要修改脚本main方法中的ip和port，获取reeminer服务的ip和port方法如下：

```
# 在部署了etcd服务器上执行
etcdctl get --prefix rock/{userId}/task/1
# 执行上述命令后，类似输出下面的信息
rock/196/task/1/602/1/196-1-602-4467
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6000","IP":"172.165.14.50","Port":19124,"GrpcPort":20000,"TcpPort":30000}
rock/196/task/1/602/2/196-1-602-1257
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6070","IP":"172.165.14.47","Port":19124,"GrpcPort":20000,"TcpPort":30000}
rock/196/task/1/602/2/196-1-602-2128
{"TaskId":602,"Type":1,"Status":2,"Stage":0,"RetryCnt":0,"LeaseId":"47c9904828ad6052","IP":"172.165.14.45","Port":19124,"GrpcPort":20000,"TcpPort":30000}
......

# 从输出中选择集群任意一个节点
获取一组ip和port，如：ip:172.165.14.50，port:19124
```

​	修改inc_rule_dig/auto_test/*.json文件中的dataSources配置项，reeminer服务会根据dataSources配置项加载数据库中的数据

```
# dataSources配置项
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
  
根据(1)中PostgreSQL实际配置信息修改host、port、databaseName、user和password项
```

​	修改inc_rule_dig/auto_test/*.xlsx文件，这里配置实际的实验参数，注意dataset name列，填写数据集在PostgreSQL实际的表名(eg: inc_rds.hospital)

(3) 直接运行上述python脚本



