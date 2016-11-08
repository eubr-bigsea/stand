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
All configuration is defined in a JSON file, with the following structure:

```
{
    "port": 3320,
    "servers": {
        "database_url": "mysql+pymysql://user:password@server:port/database",
        "environment": "prod",
        "redis_server": "redis_server"
    },
    "services": {
        "tahiti": {
            "url": "http://tahiti_server/tahiti",
            "token": "authorization_token"
        }
    }
}
```
## Database creation

If the database user specified in the `database_url` parameter in configuration file has permission of creating tables, 
they are created automatically after starting Stand. Otherwise, you have to execute the SQL script located in the folder |FIXME|.

## Running

```
cd <download_dir>
python stand/app_api.py -c <path_of_configuration_file>
```
Service will run on port 3320 (default).

## API documentation

FIXME To be written
