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
All configuration is defined in a Yaml file, with the following structure:

```
stand:
    debug: true
    servers:
        database_url: mysql+pymysql://user:password@server:port/database
        redis_url: redis://redis_server:6379
    services:
        tahiti:
            url: http://server/tahiti
            auth_token: "authorization_token"
    config:
        SQLALCHEMY_POOL_SIZE: 10
        SQLALCHEMY_POOL_RECYCLE: 240
```
## Database creation

If the database user specified in the `database_url` parameter in configuration file has permission of creating tables, 
they are created automatically after starting Stand. Otherwise, you have to execute the SQL script located in the folder |FIXME|.

## Running

```
cd <download_dir>
export STAND_CONFIG_FILE=<path for Yaml configuration file>
python stand/app.py
```
Service will run on port 5000 (default).

## API documentation

FIXME To be written

## Redis usage

Stand uses Redis as a job control storage and to support asynchronous
communication with Lemonade Juicer. The following types are used:

**Redis type** | **Name**  | **Purpose** 
------------|-------|---------
 Hash       | job_N | Controls the state of the job. Used to prevent starting a already canceled job (status) or to indicate that it requires a restart in the infrastructure
 List       | start | Used as a blocking queue, defines the order of jobs to be started by Juicer
 List       | stop  | Used as a blocking queue, defines the order of job to be stopped by Juicer

