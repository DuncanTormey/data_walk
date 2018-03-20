#!/usr/bin/env python
# Author: Duncan Tormey
# Email: dut@stowers.org or duncantormey@gmail.com
import pandas as pd
from glob import glob
import boto3
import tarfile
import os
import subprocess

accessKeys = pd.read_csv('/n/projects/dut/general_projects/data_walk/data/accessKeys.csv')

conn = boto3.session.Session(
    aws_access_key_id=accessKeys['Access key ID'].iloc[0],
    aws_secret_access_key=accessKeys['Secret access key'].iloc[0]
)

bucket = "PB_Stowers_Sequecing"

ngsBucket = conn.resource('s3').Bucket(bucket)

ordersInNgsBucket = [order.key for order in ngsBucket.objects.filter(Delimiter='/')]

uploadTargets = glob('/n/analysis/Baumann/*/*')

for target in uploadTargets:
    tarpath = '/scratch/dut/{}.tar.gz'.format(os.path.basename(target))
    tarname = os.path.basename(tarpath)

    if tarname not in ordersInNgsBucket:
        print('*' * 100)
        print("{} not found in bucket: {}. \nTaring and gzipping {}...".format(tarname, bucket, target))

        with tarfile.open(tarpath, mode="w:gz") as archive:
            archive.add(target, recursive=True)

        print('Done: {} tarred and gzipped to {}. \nChecking md5sum...'.format(target, tarpath))

        md5fileName = '{}.md5sum.txt'.format(tarname)
        md5filePath = '/n/projects/dut/general_projects/data_walk/data/{}.md5sum.txt'.format(md5fileName)
        subprocess.call(
            'md5sum {} > {}'.format(tarpath, md5filePath),
            shell=True)

        print('Done: md5sum computed... Uploading data and checksum to s3....')

        ngsBucket.upload_file(Filename=tarpath, Key=tarname)
        ngsBucket.upload_file(Filename=md5filePath, Key=md5fileName)

        print('Done: {} uploaded to s3 bucket: {}. \nRemoving local archive...'.format(tarname, bucket))

        os.remove(tarpath)

        print('Done: {} removed.'.format(tarpath))
        print('')

    else:
        pass
