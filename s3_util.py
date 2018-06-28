#!/usr/bin/env python3
"""
Utilies for reading and downloading files from Amazon S3
"""

import csv
import boto
import os

def read_csv_file(bucket_name, folder_name, file_name, aws_access_key, aws_secret_access_key):
    """
    Read csv file stored in S3 bucket, and create dictinary to store the file name 
    and bounding box as key-value pairs

    param: bucket_name: bucket name
           folder_name: folder name
           file_name:   file name
           aws_access_key:   AWS access key
           aws_secret_access_key:  AWS secret key
    return:  label_dict: a dictionary with file name as key and list of 
             bounding box coordinates as values
    """
    
    # Establish connection to S3
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bucket = conn.get_bucket(bucket_name, validate=False)
    # Get the key
    key = bucket.get_key(folder_name + file_name)
    # Read the value of a key as a string
    data = key.get_contents_as_string()

    # Store it into a string, the split characters are '\n' and use csv.excel
    reader = csv.reader(data.decode("utf-8").split('\n'), dialect=csv.excel)
    label_dict = {}
    header = next(reader) # skip the header of the csv file
    for row in reader:
        if len(row) >0:
            key_dict = row[0]
            key_value = ([int(row[4]), int(row[5]), int(row[6]), int(row[7])], row[3])
            label_dict[key_dict] = key_value
    return label_dict




def get_file_list(bucket_name, folder_name, aws_access_key, aws_secret_access_key):
    """
    Get the list of names of the image files stored in a S3 bucket

    param: bucket_name:
           folder_name:
           file_name:
           aws_access_key
           aws_secret_access_key:  
    return:  file_list: list of file names in a S3 bucket
             bucket: connection to S3
    """
    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
    bucket = conn.get_bucket(bucket_name, validate=False)
    keys = bucket.list(prefix=folder_name)
    file_list = [key.name for key in keys]
    
    return file_list, bucket



def fetch_data(file_name, bucket):
    """
    Fetch data with the given file_name key and pass along the contents as a string.

    :param file_name: An s3 key path string.
            bucket:  bucket from boto.connect_s3
    :return: data:  data is the contents of the 
        file as string (bytes). 
    """
    k = bucket.get_key(file_name)
    data = k.get_contents_as_string()
    
    return data


def download_model_files(bucket_name, folder_name, aws_access_key, aws_secret_access_key,
                         download_path):
     """
     Download from S3 bucket model files into cluster machines 
     param: bucket_name: bucket name
            folder_name: folder name
            file_name:   file name
            aws_access_key:   AWS access key
            aws_secret_access_key:  AWS secret key
            download_path: path to store the models
     return: none      
     """
     
     
     download_path = download_path + folder_name
     #download_path = download_path
     # Create the directory if not existed
     if not os.path.exists(download_path):
             os.makedirs(download_path)
     conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
     bucket = conn.get_bucket(bucket_name, validate=False)
     keys = bucket.list(prefix=folder_name)
     
     # Download model files
     for key in keys:
        file_name = key.name
        print(file_name)
        if file_name.startswith(folder_name):
            file_name = file_name[len(folder_name):]
        if len(file_name)>0:    
            key.get_contents_to_filename(download_path + file_name)        
     
    
    
    