import requests
import argparse
import sys
import tempfile
import zipfile
import json
import os
import datetime

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def checkOptions():
    parser = argparse.ArgumentParser(description = 'Imply Cluster Review - Data Collector')
    parser.add_argument('Customer', action='store', type=str, help='Enter a customer name')
    parser.add_argument('-r', '--router', action='store', type=str, dest='router', help='Router URL with protocol and port')
    parser.add_argument('-k', '--kerberos', action='store_true', dest='kerberos', help='''
                        Enable kerberos authentication (must already have valid ticket''')
    parser.add_argument('-a','--auth', action='store_true', dest='auth', help='Enable basic authentication')
    parser.add_argument('-u', '--user', action='store', type=str, dest='user', help='Username')
    parser.add_argument('-p', '--password', action='store', type=str, dest='password', help='Password')
    parser.add_argument('-t', '--token', action='store', type=str, dest='token', help='Token (like YRtWa... - no "Basic"')
    args = parser.parse_args()
    return args


def getRequest(URL):
    fullURL = options.router + URL
    if options.kerberos:
        req = requests.get(fullURL, auth=kerberos_auth, verify=False)

    elif options.auth:
        headers = {'Authorization': 'Basic ' + options.token}
        req = requests.get(fullURL, headers=headers, verify=False)

    else:
        req = requests.get(fullURL, verify=False)
    try:
        JSON = req.json()
    except json.decoder.JSONDecodeError:
        JSON = {}
    return JSON
    

def postRequest(URL, query):
    fullURL = options.router + URL
    if options.kerberos:
        headers = {'Content-Type': 'application/json'}
        req = requests.post(fullURL, headers=headers, json=qery, auth=kerberos_auth, verify=False)

    elif options.auth:
        headers = {'Content-Type': 'application/json', 'Authorization': 'Basic ' + options.token}
        req = requests.post(fullURL, headers=headers, json=query, verify=False)
    else:
        headers = {'Content-Type': 'application/json'}
        req = requests.post(fullURL, headers=headers, json=query, verify=False)

    JSON = req.json()
    return JSON


def getSegments():
    queryJSON = {'query': 'SELECT "datasource", count("segment_id") as segmentCount, AVG("size") as avgSize, avg("num_rows") as avgRows FROM sys.segments  GROUP BY "datasource"'}
    URL = '/druid/v2/sql'
    segmentJSON = postRequest(URL, queryJSON)
    return segmentJSON


def getServers():
    queryJSON = {'query': """
        SELECT "server" AS "service", "server_type" AS "service_type", "tier", "host", "plaintext_port", "tls_port", "curr_size", "max_size",  
          (CASE "server_type" 
            WHEN \'coordinator\' THEN 8  
            WHEN \'overlord\' THEN 7 
            WHEN \'router\' THEN 6 
            WHEN \'broker\' THEN 5  
            WHEN \'historical\' THEN 4 
            WHEN \'indexer\' THEN 3 
            WHEN \'middle_manager\' THEN 2 
            WHEN \'peon\' THEN 1 
            ELSE 0 
            END ) AS "rank" 
        FROM sys.servers 
        ORDER BY "rank" DESC, 
        "service" DESC
        """
      }
    URL = '/druid/v2/sql'
    serverJSON = postRequest(URL, queryJSON)
    return serverJSON


def getCompaction():
    URL = '/druid/coordinator/v1/config/compaction'
    compactionJSON = getRequest(URL)
    return compactionJSON


def getSupervisors():
    URL = '/druid/indexer/v1/supervisor?full'
    supervisorsJSON = getRequest(URL)
    return supervisorsJSON


def getRetention():
    URL = '/druid/coordinator/v1/rules?full'
    retentionJSON = getRequest(URL)
    return retentionJSON

def getDatasources():
    URL = '/druid/coordinator/v1/metadata/datasources?full'
    datasourceJSON = getRequest(URL)
    return datasourceJSON

def getCoordinatorSettings():
    URL = '/druid/coordinator/v1/config'
    coordinatorJSON = getRequest(URL)
    return coordinatorJSON

def getOverlordSettings():
    URL = '/druid/indexer/v1/worker'
    overlordJSON = getRequest(URL)
    return overlordJSON

def getLookups():
    URL = '/druid/coordinator/v1/lookups/config/all'
    lookupJSON = getRequest(URL)
    return lookupJSON

def main():
    global options
    options = checkOptions()

    if options.router is None:
        print('Please specify a druid Router like "-r https://localhost:8888"')
        sys.exit(2)

    if options.kerberos:
        from requests_kerberos import HTTPKerberosAuth, REQUIRED
        global kerberos_auth
        kerberos_auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, sanitize_mutual_error_response=False)

    if options.auth:
        if not ((options.user and options.password) or options.token):
            print('Please enter username and password, or token')
            sys.exit(2)
        else:
            if not options.token:
                import base64
                userPass = bytes(options.user + ':' + options.password, encoding='utf-8')
                options.token = base64.b64encode(userPass).decode("utf-8")
            

    segmentJSON = getSegments()
    compactionJSON = getCompaction()
    supervisorsJSON = getSupervisors()
    retentionJSON = getRetention()
    datasourceJSON = getDatasources()
    coordinatorJSON = getCoordinatorSettings()
    overlordJSON = getOverlordSettings()
    serverJSON = getServers()
    lookupJSON = getLookups()
    
    fileDetails = options.Customer + '_' + datetime.date.today().isoformat()
    with zipfile.ZipFile(fileDetails + '.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(os.path.join(fileDetails, 'segments.json'), json.dumps(segmentJSON))
        archive.writestr(os.path.join(fileDetails, 'compaction.json'), json.dumps(compactionJSON))
        archive.writestr(os.path.join(fileDetails, 'supervisors.json'), json.dumps(supervisorsJSON))
        archive.writestr(os.path.join(fileDetails, 'retention.json'), json.dumps(retentionJSON))
        archive.writestr(os.path.join(fileDetails, 'datasources.json'), json.dumps(datasourceJSON))
        archive.writestr(os.path.join(fileDetails, 'coordinator.json'), json.dumps(coordinatorJSON))
        archive.writestr(os.path.join(fileDetails, 'overlord.json'), json.dumps(overlordJSON))
        archive.writestr(os.path.join(fileDetails, 'servers.json'), json.dumps(serverJSON))
        archive.writestr(os.path.join(fileDetails, 'lookups.json'), json.dumps(lookupJSON))


if __name__ == '__main__':
    main()