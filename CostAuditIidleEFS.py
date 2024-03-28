import boto3
import json
import pandas as pd
from datetime import datetime, timedelta
import xlsxwriter
class EFS:

    def file_systemsIds(self,region):
        efs = boto3.client('efs', region_name=region)
        response = efs.describe_file_systems()
        FS=[]
        for FileSysId in response['FileSystems']:
            FS.append(FileSysId['FileSystemId'])
        return FS

    def DateTimeFilesystem(region):
        efs = boto3.client('efs', region_name=region)
        response = efs.describe_file_systems()
        Date = []
        Time = []
        Timezone = []
        for FileSysId in response['FileSystems']:
            b = str(FileSysId['CreationTime'])
            c = b[0:10]  # for date
            d = b[11:19] #for time
            e = b[19:25] #for timezone
            Date.append(c)
            Time.append(d)
            Timezone.append(e)

        return Date, Time, Timezone

    def connected_clients(self,file_system_ids,region):
        cloudwatch = boto3.client('cloudwatch',region_name=region)
        countConnected = 0
        for fs_id in file_system_ids:
            response = cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        'Id': 'connected_clients',
                        'MetricStat': {
                            'Metric': {
                                'Namespace': 'AWS/EFS',
                                'MetricName': 'TotalIOBytes',
                                'Dimensions': [
                                    {
                                        'Name': 'FileSystemId',
                                        'Value': fs_id
                                    },
                                ]
                            },
                            'Period': 300,
                            'Stat': 'Sum',
                        },
                        'ReturnData': True,
                    },
                ],
                StartTime=datetime.now() - timedelta(days=14),
                EndTime=datetime.now(),
            )
            if len(response['MetricDataResults'][0]['Values']) > 0:
                countConnected += 1
        return countConnected

    def file_systemunusedIds(self, file_system_idsK, region):
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        efs = boto3.client('efs')
        response2 = efs.describe_file_systems()
        file_systems = []
        Date = []
        Time = []
        TimeZone = []
        for fs_id in file_system_idsK:
            response = cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        'Id': 'connected_clients',
                        'MetricStat': {
                            'Metric': {
                                'Namespace': 'AWS/EFS',
                                'MetricName': 'TotalIOBytes',
                                'Dimensions': [
                                    {
                                        'Name': 'FileSystemId',
                                        'Value': fs_id
                                    },
                                ]
                            },
                            'Period': 300,
                            'Stat': 'Sum',
                        },
                        'ReturnData': True,
                    },
                ],
                StartTime=datetime.now() - timedelta(days=14),
                EndTime=datetime.now(),
            )
            if len(response['MetricDataResults'][0]['Values']) == 0:
                file_systems.append(fs_id)
                for FileSysId in response2['FileSystems']:
                    if FileSysId['FileSystemId'] == fs_id:
                        b = str(FileSysId['CreationTime'])
                        c = b[0:10]  # for date
                        d = b[11:19]
                        e = b[19:25]
                        Date.append(c)
                        Time.append(d)
                        TimeZone.append(e)
        return file_systems, Date, Time, TimeZone
    def count_unused_efs(self, region):
        file_systems = self.file_systemsIds(region)
        count = len(file_systems)
        count_connected = self.connected_clients(file_systems,region)
        count_unused = count - count_connected
        return count_unused
    def filesystemIds(self,region):
        file_systems = self.file_systemsIds(region)
        IdsOfFileSystem, date1, time1,timezone1 = self.file_systemunusedIds(file_systems,region)
        return IdsOfFileSystem, date1, time1, timezone1
    def calculate_price(self,region):
        pricing = boto3.client('pricing')
        response = pricing.get_products(
            ServiceCode='AmazonEFS',
            Filters=[
                {
                    'Type': 'TERM_MATCH',
                    'Field': 'regionCode',
                    'Value': region
                },
            ],
            MaxResults=100
        )
        data = response['PriceList']

        for dataNew in data:
            data1 = json.loads(dataNew)
            if 'storageClass' in data1['product']['attributes']:
                if data1['product']['attributes']['storageClass'] == 'General Purpose':
                    data2 = list(data1['terms']['OnDemand'].keys())
                    data3 = list(data1['terms']['OnDemand'][data2[0]]['priceDimensions'].keys())
                    data4 = data1['terms']['OnDemand'][data2[0]]['priceDimensions'][data3[0]]
                    return float(data4['pricePerUnit']['USD'])


def main():
    client = boto3.client('ec2')
    regions = [region['RegionName'] for region in client.describe_regions()['Regions']]
    workbook = xlsxwriter.Workbook('output.xlsx')
    workbook.close()
    existing_data = pd.read_excel('output.xlsx')
    region = []
    for region1 in regions:
        region.append(region1)
    for region_new in region:
        efs = EFS()
        unused_efs = efs.count_unused_efs(region_new)
        filesystem1,date1,time1,timezone1 = efs.filesystemIds(region_new)
        filesystem = str(filesystem1).replace('[', '').replace(']', '').replace('\'', '').replace('\'', '')
        date2 = str(date1).replace('[', '').replace(']', '').replace('\'', '').replace('\'', '')
        time2 = str(time1).replace('[', '').replace(']', '').replace('\'', '').replace('\'', '')
        timezone2 = str(timezone1).replace('[', '').replace(']', '').replace('\'', '').replace('\'', '')
        price = efs.calculate_price(region_new)
        finalPrice = price * unused_efs
        data = {
            'Region': [region_new],
            'Total No. of Idle EFS': [unused_efs],
            'File system Id\'s': [filesystem],
            'Monthly Cost': [finalPrice],
            'CreationDate': [date2],
            'CreationTime': [time2],
            'Timezone': [timezone2],
            'finding': 'unused'
        }
        df = pd.DataFrame(data)
        existing_data = pd.concat([existing_data,df], ignore_index=True)
        existing_data.to_excel('output.xlsx', index=False)


if __name__ == "__main__":
    main()





