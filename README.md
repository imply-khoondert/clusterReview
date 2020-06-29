# clusterReview

## Generate a data file

```
# python3 DataCollector.py -h

usage: DataCollector.py [-h] [-r ROUTER] [-k] [-a] [-u USER] [-p PASSWORD]
                        [-t TOKEN]
                        Customer

Imply Cluster Review - Data Collector

positional arguments:
  Customer              Enter a customer name

optional arguments:
  -h, --help            show this help message and exit
  -r ROUTER, --router ROUTER
                        Router URL with protocol and port
  -k, --kerberos        Enable kerberos authentication (must already have
                        valid ticket
  -a, --auth            Enable basic authentication
  -u USER, --user USER  Username
  -p PASSWORD, --password PASSWORD
                        Password
  -t TOKEN, --token TOKEN
                        Token (like YRtWa... - no "Basic"
```

## Generate a report file (.xlsx)
```
# python3 SheetGen.py -h
usage: SheetGen.py [-h] [-f FILENAME] Customer

Imply Cluster Review - XLSX Generator

positional arguments:
  Customer              Enter a customer name

optional arguments:
  -h, --help            show this help message and exit
  -f FILENAME, --fileName FILENAME
                        Enter the location of the collected datafile
```
