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

    def file_systemunusedIds(self,file_system_idsK,region):
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        file_systems = []
        for fs_id in file_system_idsK:
            response = cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        'Id': 'connected_clients',
                        'MetricStat': {
                            'Metric': {
                                'Namespace': 'AWS/EFS',
                                'MetricName': 'ClientConnections',
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
        return file_systems
    def count_unused_clients(self,region):
        file_systems = self.file_systemsIds(region)
        count = len(file_systems)
        count_connected = self.connected_clients(file_systems,region)
        count_unused = count - count_connected
        return count_unused
    def filesystemIds(self,region):
        file_systems = self.file_systemsIds(region)
        IdsOfFileSystem = self.file_systemunusedIds(file_systems,region)
        return IdsOfFileSystem
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
        unused_clients = efs.count_unused_clients(region_new)
        filesystem = efs.filesystemIds(region_new)
        # printing unused clients
        print(unused_clients)
        price = efs.calculate_price(region_new)
        print(price)
        finalPrice = price * unused_clients
        Message = 'There are  {file} idle efs'.format(file=unused_clients)
        Message2 = 'Region {file}'.format(file=region_new)
        Message3 = 'The total price for the {file} idle efs'.format(file=finalPrice)
        print(Message)
        print(Message2)
        print(Message3)
        data = {
            'No of Idle EFS': [unused_clients],
            'Region': [region_new],
            'Filesystem': [filesystem],
            'Total Price': [finalPrice]
        }
        df = pd.DataFrame(data)
        existing_data = pd.concat([existing_data,df], ignore_index=True)
        existing_data.to_excel('output.xlsx', index=False)


if __name__ == "__main__":
    main()





