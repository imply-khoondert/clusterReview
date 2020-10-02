import xlsxwriter
import json
import argparse
import datetime
import zipfile
import os

def checkOptions():
    parser = argparse.ArgumentParser(description = 'Imply Cluster Review - XLSX Generator')
    parser.add_argument('Customer', action='store', type=str, help='Enter a customer name')
    parser.add_argument('-f', '--fileName', action='store', dest='filename', help='Enter the location of the collected datafile')    
    args = parser.parse_args()
    return args


def segmentToSheet(ws, data):
    ws.write(0, 0, "DataSource")
    ws.write(0, 1, "Segment Count")
    ws.write(0, 2, "Average Size")
    ws.write(0, 3, "Average Rows")
    sheetRow = 1
    sheetCol = 0
    for item in data:
        ws.write(sheetRow, sheetCol, item['datasource'])
        ws.write(sheetRow, sheetCol + 1, item['segmentCount'])
        ws.write(sheetRow, sheetCol + 2, item['avgSize'])
        ws.write(sheetRow, sheetCol + 3, item['avgRows'])
        sheetRow += 1
    return


def compactionToSheet(ws, data):
    ws.write(0, 0, "DataSource")
    ws.write(0, 1, "Task Priority")
    ws.write(0, 2, "Input Segment Size Bytes")
    ws.write(0, 3, "Max Rows Per Segment")
    ws.write(0, 4, "Skip Offset From Latest")
    ws.write(0, 5, "Tuning Config")
    ws.write(0, 6, "Task Context")
    sheetRow = 1
    sheetCol = 0
    for item in data['compactionConfigs']:
        ws.write(sheetRow, sheetCol, item['dataSource'])
        ws.write(sheetRow, sheetCol + 1, item['taskPriority'])
        ws.write(sheetRow, sheetCol + 2, item['inputSegmentSizeBytes'])
        ws.write(sheetRow, sheetCol + 3, item['maxRowsPerSegment'])
        ws.write(sheetRow, sheetCol + 4, item['skipOffsetFromLatest'])
        ws.write(sheetRow, sheetCol + 5, str(item['tuningConfig']))
        ws.write(sheetRow, sheetCol + 6, item['taskContext'])
        sheetRow += 1
    return


def retentionToSheet(ws, data):
    ws.write(0, 0, "DataSource")
    ws.write(0, 1, "Rule")
    ws.write(0, 2, "Period")
    ws.write(0, 3, "Future")
    ws.write(0, 4, "Tier")
    ws.write(0, 5, "Replicas")
    sheetRow = 1
    sheetCol = 0
    for ds, rules in data.items():
        ws.write(sheetRow,sheetCol, ds)
        sheetRow += 1
        sheetCol += 1
        for item in rules:
            ws.write(sheetRow, sheetCol, item['type'])
            try:
                ws.write(sheetRow, sheetCol + 1, item['period'])
                ws.write(sheetRow, sheetCol + 2, item['includeFuture'])
                for tier, reps in item['tieredReplicants'].items():
                    ws.write(sheetRow, sheetCol + 3, tier)
                    ws.write(sheetRow, sheetCol + 4, reps)
                    sheetRow += 1
            except:
                sheetRow += 1
        sheetCol = 0

    return


def supervisorsToSheet(ws, data):
    ws.write(0, 0, "DataSource")
    ws.write(0, 1, "Stream")
    ws.write(0, 2, "ID")
    ws.write(0, 3, "Type")
    ws.write(0, 4, "State")
    ws.write(0, 5, "Task Count")
    ws.write(0, 6, "Replicas")
    ws.write(0, 7, "Task Duration")
    ws.write(0, 8, "Records Per Fetch")
    ws.write(0, 9, "Max rows in mem")
    ws.write(0, 10, "Max rows per segment")
    sheetRow = 1
    sheetCol = 0
    for item in data:
        ws.write(sheetRow, sheetCol, item['spec']['spec']['dataSchema']['dataSource'])
        ws.write(sheetRow, sheetCol + 1, item['spec']['ioConfig']['stream'])
        ws.write(sheetRow, sheetCol + 2, item['id'])
        ws.write(sheetRow, sheetCol + 3, item['spec']['type'])
        ws.write(sheetRow, sheetCol + 4, item['detailedState'])
        ws.write(sheetRow, sheetCol + 5, item['spec']['ioConfig']['taskCount'])
        ws.write(sheetRow, sheetCol + 6, item['spec']['ioConfig']['replicas'])
        ws.write(sheetRow, sheetCol + 7, item['spec']['ioConfig']['taskDuration'])
        try:
            ws.write(sheetRow, sheetCol + 8, item['spec']['ioConfig']['recordsPerFetch'])
        except:
            pass
        ws.write(sheetRow, sheetCol + 9, item['spec']['tuningConfig']['maxRowsInMemory'])
        ws.write(sheetRow, sheetCol + 10, item['spec']['tuningConfig']['maxRowsPerSegment'])
        sheetRow += 1
    return


def serversToSheet(ws, data):
    ws.write(0, 0, "Service")
    ws.write(0, 1, "Service Type")
    ws.write(0, 2, "Tier")
    ws.write(0, 3, "Host")
    ws.write(0, 4, "Plaintext Port")
    ws.write(0, 5, "TLS Port")
    ws.write(0, 6, "Current Size")
    ws.write(0, 7, "Max Size")
    ws.write(0, 8, "Rank")
    sheetRow = 1
    sheetCol = 0
    for item in data:
        ws.write(sheetRow, sheetCol, item['service'])
        ws.write(sheetRow, sheetCol + 1, item['service_type'])
        ws.write(sheetRow, sheetCol + 2, item['tier'])
        ws.write(sheetRow, sheetCol + 3, item['host'])
        ws.write(sheetRow, sheetCol + 4, item['plaintext_port'])
        ws.write(sheetRow, sheetCol + 5, item['tls_port'])
        ws.write(sheetRow, sheetCol + 6, item['curr_size'])
        ws.write(sheetRow, sheetCol + 7, item['max_size'])
        ws.write(sheetRow, sheetCol + 8, item['rank'])
        sheetRow += 1
    return

def main():
    global options
    options = checkOptions()
    
    # Create an empty workbook
    workbook = xlsxwriter.Workbook(options.Customer + '_' + datetime.date.today().isoformat() + '.xlsx')

    zipBase = os.path.basename(options.filename)
    zipDir = os.path.splitext(zipBase)[0]
    
    with zipfile.ZipFile(options.filename) as zipper:
        with zipper.open(zipDir + '/' + 'segments.json') as segments:
            segmentJSON = json.load(segments)
            segmentSheet = workbook.add_worksheet('Segments')
            segmentToSheet(segmentSheet, segmentJSON)
        with zipper.open(zipDir + '/' + 'compaction.json') as compaction:
            compactionJSON = json.load(compaction)
            compactionSheet = workbook.add_worksheet('Compaction')
            compactionToSheet(compactionSheet, compactionJSON)
        with zipper.open(zipDir + '/' + 'retention.json') as retention:
            retentionJSON = json.load(retention)
            retentionSheet = workbook.add_worksheet('Retention')
            retentionToSheet(retentionSheet, retentionJSON)
        with zipper.open(zipDir + '/' + 'supervisors.json') as supervisors:
            supervisorsJSON = json.load(supervisors)
            supervisorsSheet = workbook.add_worksheet('Supervisors')
            supervisorsToSheet(supervisorsSheet, supervisorsJSON)
        with zipper.open(zipDir + '/' + 'servers.json') as servers:
            serversJSON = json.load(servers)
            serversSheet = workbook.add_worksheet('Servers')
            serversToSheet(serversSheet, serversJSON)


    workbook.close()

if __name__ == '__main__':
    main()
