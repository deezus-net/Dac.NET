![](https://github.com/deezus-net/Dac.Net/workflows/test/badge.svg)
# DAC
DAC (Database As Code) is a tool to manage the database table structure with yaml  

## How to use
```
dac [command] [option]
```

## command
| Command | Description |
|:---|:---|
| extract | Connect to the database and export the tables as yaml |
| create | Create tables from yaml |
| recreate | drops the existing tables and reconstructs the tables from yaml |
| update | Compare the differences between yaml and the database and update tables and columns |
| diff | Display difference between yaml and database |

## option
| Options | Description | Examples etc. | |
|:---|:---|:---|:---:|
| --hosts &lt;hosts&gt; | Connection information to the database yaml file path | hosts.yml | |
| --host &lt;host&gt; | database host (connection destination name when -f is specified) | localhost | * ||
| --type &lt;type&gt; | database type | mysql, pgsql, mssql | * |
| --user &lt;user&gt;| users connecting to the database | | * |
| --password &lt;password&gt; | Password for connecting to the database | | * |
| --database &lt;database&gt; | database name | | * |
| --input &lt;input&gt; | input yaml file path | db.yml | |
| --output &lt;output&gt; | output directory of extract directory | | |
| --query | Output to screen without executing query at create, recreate, update | | |
| --dry-run |  executing query withount committing at create, recreate, update | | |
| --help | show help| | |

*--hosts option is required for unspecified

## Example of use

### extract
When specifying connection information with argument
```
dac extract --host localhost --type mysql --user root --password password --database dac --output .
```
When setting connection information in a file
```
dac extract --hosts hosts.yml --output .
```
--------------
  
### create
When specifying connection information with argument
```
dac create --host localhost --type mysql --user root --password password --database dac --input db.yml
```
When setting connection information in a file
```
dac create --hosts hosts.yml --input db.yml
```
When query is displayed
```
dac create --hosts hosts.yml --input db.yml --query
```
--------------
  
### recreate
When specifying connection information with argument
```
dac recreate --host localhost --type mysql --user root --password password --database dac --input db.yml
```
When setting connection information in a file
```
dac recreate --hosts hosts.yml --input db.yml
```
When query is displayed
```
dac recreate --hosts hosts.yml --input db.yml --query
```
--------------
  
### update
When specifying connection information with argument
```
dac update --hosts localhost --type mysql --user root --password password --database dac --input db.yml
```
When setting connection information in a file
```
dac update --hosts hosts.yml --input db.yml
```
When query is displayed
```
dac update --hosts hosts.yml --input db.yml --query
```
--------------
  
### diff
When specifying connection information with argument
```
dac diff --host localhost --type mysql --user root --password password --database dac --input db.yml
```
When setting connection information in a file
```
dac diff --hosts hosts.yml --input db.yml
```

## About hosts
You can list multiple connection information in yml  
You can mix different types of databases as follows  
If you do not specify a name with the --host option, the command is executed for all destinations
```yaml: hosts.yml
server 1:
  type: mysql
  host: localhost
  user: db_user_1
  password: password
  database: dac
 
server 2:
  type: pgsql
  host: localhost
  user: db_user_2
  password: password
  database: dac

server3:
  type: mssql
  host: localhost
  user: sa
  password: !Passw0rd
  database: dac
```

### extract example
When doing to all connection destinations  
A file is created for server1.yml, server2.yml, server3.yml and connection destination
```
dac extract --input hosts.yml --output .
```

When specifying the connection destination name  
It extracts only 'server1' and creates server1.yml
```
dac extract --input hosts.yml --host server1 --output .
```