# Lemonade Stand
[logo]: docs/img/stand.png "Lemonade Stand"

![alt text][logo]

Stand is the execution API for Lemonade project. It provides methods to run workflows and get their execution status.

## Installation
### Getting the source code

 `git clone https://github.com/eubr-bigsea/stand.git`

### Requirements
 - Python 2.7.x
 - Redis 3.0.x
 - MySQL server > 5.0

 To install python libraries, run:

 ```
 cd <donwload_dir>
 pip install -r requirements.txt
 ```
## Configuration
All configuration is defined in a Yaml file `stand-config.yaml`,
with the following structure:

```
stand:
    debug: true
    port: 3323
    servers:
        database_url: mysql+pymysql://user:password@server:port/database
        redis_url: redis://redis_server:6379
    services:
        tahiti:
            url: http://server/tahiti
            auth_token: "authorization_token"
    config:limonero
        SQLALCHEMY_POOL_SIZE: 10
        SQLALCHEMY_POOL_RECYCLE: 240
```

You will find the template above in `conf/stand-config.yaml.template`.

## Database creation

Open a connection to the MySQL server and create a new database having the same
name as specified in the `database_url` parameter in the configuration file:
```
CREATE DATABASE stand CHARACTER SET utf8 COLLATE utf8_general_ci;
```

After, grant permissions to the user specified in the configuration:
```
GRANT ALL ON stand.* TO 'user'@'%' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
```

Change to the source code directory and execute the following command:
```
PYTHONPATH=. STAND_CONFIG=./stand-config.yaml python stand/manage.py db upgrade
```
This command will create the tables in the database.

Finally, go back to MySQL and execute the following command in the new database, changing parameters accordinately:
 ```
 USE stand;

 INSERT INTO `stand`.`cluster`
   (`id`, `name`, `description`,  `enabled`, `type`, `address`)
   VALUES (1, 'Cluster', 'Example cluster', 1, 'SPARK_LOCAL', 'spark://address:7000');
 ```


## Running

```
cd <download_dir>
./sbin/stand-daemon.sh start
```
Service will run on port 3323 (default).

You can check the stand daemon status with:
```
./sbin/stand-daemon.sh status
```

You can stop the stand daemon with:
```
./sbin/stand-daemon.sh stop
```
 ## Using docker
 In order to build the container, change to source code directory and execute the command:
 ```
 docker build -t bigsea/stand .
 ```
 Repeat [config](#config) stop and run using config file
 ```
 docker run \
    -v $PWD/stand-config.yaml:/usr/src/app/stand-config.yaml \
    -p 3323:3323 \
    bigsea/stand
```

## API documentation

**Endpoint** | **Purpose**
-------------|-------------
/jobs/<int:job_id> | JobDetailApi
/jobs/<int:job_id>/stop | JobStopActionApi
/jobs/<int:job_id>/lock | JobLockActionApi
/jobs/<int:job_id>/unlock | JobUnlockActionApi
/clusters/<int:cluster_id> | ClusterDetailApi

## Redis usage

Stand uses Redis as a job control storage and to support asynchronous
communication with Lemonade Juicer. The following types are used:

**Redis type** | **Name**  | **Purpose** 
------------|-------|---------
 Hash       | job_N | Controls the state of the job. Used to prevent starting a already canceled job (status) or to indicate that it requires a restart in the infrastructure
 List       | start | Used as a blocking queue, defines the order of jobs to be started by Juicer
 List       | stop  | Used as a blocking queue, defines the order of job to be stopped by Juicer

