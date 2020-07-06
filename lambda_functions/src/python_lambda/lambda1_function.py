import xarray as xr
import boto3
import json
import os
import time
from pathlib import Path
import shutil
from boto3.s3.transfer import TransferConfig


def s3_client():
    s3_client = boto3.client('s3')
    """ :type : pyboto3.s3 """
    return s3_client


def s3_resource():
    s3_resource = boto3.resource('s3')
    return s3_resource


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    bucket_to_name.copy(copy_source, file_name)


def upload_file(filename, bucket, object_name):
    print(filename, bucket, object_name)
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)

    s3_resource().meta.client.upload_file(filename, bucket, object_name,
                                          ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/csv',
                                                     'ContentEncoding': 'gzip'},
                                          Config=config)


def create_tmpdirs():
    p1 = Path('/tmp/temperature_directory')
    p2 = Path('/tmp/wind_directory')
    p1.mkdir(exist_ok=True)
    p2.mkdir(exist_ok=True)


def delete_tmpdir1():
    trash_dir1 = '/tmp/temperature_directory'
    try:
        shutil.rmtree(trash_dir1)
    except OSError as e:
        print(f'Error: {trash_dir1} : {e.strerror}')


def delete_tmpdir2():
    trash_dir2 = '/tmp/wind_directory'
    try:
        shutil.rmtree(trash_dir2)
    except OSError as e:
        print(f'Error: {trash_dir2} : {e.strerror}')


def convert_to_csv():
    location = '/tmp/'
    files_in_dir = []

    # r=>root, d=>directories, f=>files
    for r, d, f in os.walk(location):
        for item in f:
            if '.nc' in item:
                filepath = location + item
                print("starting convertion to .csv for file-" + filepath)
                create_tmpdirs()
                select_variable_from_file(filepath)
                delete_tmpdir1()
                delete_tmpdir2()


def select_variable_from_file(filename):
    ds = xr.open_dataset(filename)
    for variablename in ds:
        if variablename == 'air_temperature':
            print("writing air_temperature csv file")
            air_temperature = ds['air_temperature']
            timestr = time.strftime("%Y%m%d-%H%M%S")
            outputfilename = 'air_temperature-' + timestr + '.csv.gz'
            outputfilepath = "/tmp/temperature_directory/" + outputfilename
            air_temperature.to_dataframe().to_csv(outputfilepath, compression='gzip')
            print("file written for air_temperature:" + outputfilepath)
            bucket_air = "mogrep-data-awstakehome"
            keyfile = "temperature_directory/" + outputfilename
            upload_file(outputfilepath, bucket_air, keyfile)
            print("upload air_temperature csv to bucket completed:" + keyfile)
        elif variablename == 'wind_speed':
            print("writing wind_speed csv file")
            wind_speed = ds['wind_speed']
            timestr = time.strftime("%Y%m%d-%H%M%S")
            outputfilename = 'wind_speed-' + timestr + '.csv.gz'
            outputfilepath = "/tmp/wind_directory/" + outputfilename
            wind_speed.to_dataframe().to_csv(outputfilepath, compression='gzip')
            print("file written for wind_speed:" + outputfilepath)
            bucket_wind = "mogrep-data-awstakehome"
            keyfile = "wind_directory/" + outputfilename
            upload_file(outputfilepath, bucket_wind, keyfile)
            print("upload wind_speed csv to bucket completed:" + keyfile)
        else:
            continue


def handler(event, context):
    queued_messages = event
    print(queued_messages)
    if 'Records' in queued_messages and len(queued_messages['Records']) >= 1:
        for message in queued_messages['Records']:
            notification = json.loads(message['body'])
            sns_message = json.loads(notification['Message'])
            message_attribute = sns_message['name']
            print(message_attribute)
            if message_attribute == 'air_temperature':
                bucket = s3_resource().Bucket('mogrep-data-awstakehome')
                copy_to_bucket(sns_message['bucket'], bucket, sns_message['key'])
                location = '/tmp/'
                file_name = location + sns_message['key']
                s3_client().download_file(sns_message['bucket'], sns_message['key'], file_name)
                convert_to_csv()
            elif message_attribute == 'wind_speed':
                bucket = s3_resource().Bucket('mogrep-data-awstakehome')
                copy_to_bucket(sns_message['bucket'], bucket, sns_message['key'])
                location = '/tmp/'
                file_name = location + sns_message['key']
                s3_client().download_file(sns_message['bucket'], sns_message['key'], file_name)
                convert_to_csv()
            else:
                print("no temperature or wind_speed variable")
                continue

    return {
        'statusCode': 200,
        'message': 'Function completed!'
    }