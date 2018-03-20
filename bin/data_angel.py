#!/usr/bin/env python
# Author: Duncan Tormey
# Email: dut@stowers.org or duncantormey@gmail.com
import datetime
import os
import random
import subprocess
from glob import glob

import boto3
import pandas as pd


def read_md5sum_stdout(file_path):
    with open(file_path) as fh:
        md5line = fh.readline().strip()
    md5sum = md5line.split()[0]

    return md5sum


accessKeys = pd.read_csv('../data/accessKeys.csv')

conn = boto3.session.Session(
    aws_access_key_id=accessKeys['Access key ID'].iloc[0],
    aws_secret_access_key=accessKeys['Secret access key'].iloc[0]
)

bucket = "PB_Stowers_Sequecing"

ngsBucket = conn.resource('s3').Bucket(bucket)

ordersInNgsBucket = [order.key for order in ngsBucket.objects.filter(Delimiter='/')]

testOrders = random.sample(ordersInNgsBucket, 8)

results = []

for order in testOrders:
    print('*' * 100)
    destFile = '/scratch/dut/{}'.format(order)
    md5SumUploadFile = '../data/{}.md5sum.txt'.format(order)
    uploadMd5Sum = read_md5sum_stdout(md5SumUploadFile)

    print('downloading {} from s3...'.format(order))
    ngsBucket.download_file(order, destFile)

    print('finished downloading {}. Checking md5sum...')
    md5SumDownloadFile = '../data/{}.download.md5sum.txt'.format(order)
    subprocess.call('md5sum {} > {}'.format(destFile, md5SumDownloadFile), shell=True)
    downloadMd5Sum = read_md5sum_stdout(md5SumDownloadFile)

    print('finished checking md5sum. Removing downloaded copy...')
    os.remove(destFile)
    print('finished removing {}'.format(destFile))
    print('*' * 100)
    results.append({'order': order, 'upload_md5sum': uploadMd5Sum, 'download_md5sum': downloadMd5Sum})

resultsDf = pd.DataFrame(results)
resultsDf['match'] = resultsDf['upload_md5sum'] == resultsDf['download_md5sum']

resultsDf = resultsDf[['order', 'upload_md5sum', 'download_md5sum', 'match']]

print(resultsDf)
outfile = '../data/data_angel_results_{}.tsv'.format(datetime.date.today())
resultsDf.to_csv(outfile, sep='\t', index=False)
