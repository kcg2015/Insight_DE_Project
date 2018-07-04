#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The main file of the project that implements the model 
validation pipeline
"""

import os
import findspark
findspark.init("/Users/kyleguan/spark-2.1.1-bin-hadoop2.7")
import pyspark
import numpy as np
import time
import datetime
from s3_util import *  
import db_util
from tl_detector import *
import yaml

if __name__=='__main__': 
     
     # AWS information
     aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
     aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

     # Open the YAML file that stores the parameters
     with open("para.yaml", "r") as f:
           args = yaml.load(f)
     
     # The bucket and folder information for the test images and 
     #labels
     bucket_name = args['bucket_name']
     folder_name_label = args['folder_name_label']
     folder_name_image = args['folder_name_image']
     file_name_label = args['file_name_label']
     
     # The bucket and folder information for models (pickle or protocol buffer file)
     #model_bucket_name = "models-kcguan"
     folder_name_model = args['folder_name_model']
     folder_name_version = args['folder_name_version']
     folder_name_model_full = folder_name_model + folder_name_version
     download_path = args['download_path'] 
     #local_path = download_path + model_folder_name
     
     # Database information
     db_name = args['db_name']
     tb_name = args['tb_name']
     model_name = args['model_name']
     model_version = args['model_version']
     img_w = args['img_w']
     img_h = args['img_h']
     num_partition = args['num_partition']
     
     
     # Create the database
     db_util.create_database(db_name, tb_name)
     
     # Download the related model (weights) files from S3 bucket
     # This line can be commented out if the model has already been
     # distributed to the clusters
     download_model_files(bucket_name, folder_name_model_full, aws_access_key, aws_secret_access_key,
                         download_path)
     
     # Read the label files from the S3 bucket
     label_dict = read_csv_file(bucket_name, folder_name_label, file_name_label, 
                                aws_access_key, aws_secret_access_key)
    
    # Get the list of files stored in one bucket
     image_file_list, bucket = get_file_list(bucket_name, folder_name_image, 
                                             aws_access_key, aws_secret_access_key)
     image_file_list = image_file_list[1:]
     no_image_files = len(image_file_list)
     
     
     
     # Create an SparkContext
     
     sc = pyspark.SparkContext()
    
     #Create an RDD from the list of s3 key names 
     
     file_list_rdd = sc.parallelize(image_file_list, num_partition)
     
     # Lazy evaluation to
     # 1) read images using a map job in Spark 
     # 2) carry out object detection 

     det_output = file_list_rdd.map(lambda x: fetch_data(x, bucket)) \
                          .map(lambda x: detection_output(x, folder_name_model_full))
     
     start = time.time()
     for idx, item in enumerate(det_output.take(100)):     
          
          
          detected_box = item[0]
          
          # Get the image file
          image_file_name = image_file_list[idx]
          if image_file_name.startswith(folder_name_image):
              image_file_name = image_file_name[len(folder_name_image):]
          # Read the label
          labeled_box = label_dict[image_file_name][0]
          
          # Calculate IOU
          iou = box_iou(detected_box, labeled_box)
          iou = float("{0:.2f}".format(iou))
          # Get the confidence
          conf = float("{0:.2f}".format(item[1]))
          
          color_str = item[2]

          labeled_color =  label_dict[image_file_name][1]      
          #  If IOU (intersection over union) is larger than a threshold
          #  declare the detection a success        
          success = str((iou >0.6) and (labeled_color == color_str))
          
          
          # Time stamp
          ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
          
          # Create a row to be inserted into the database
          database_row = [idx, image_file_name, img_w, 
                          img_h, model_name, model_version, 
                          ts, detected_box, labeled_box, 
                          iou, conf, color_str, success]
          db_util.insert_database(db_name, tb_name, database_row) 
          
     # Print out all the rows of database for debugging, can be commented out.
     db_util.print_database(db_name, tb_name)     
     
     end = time.time()
     # This line can be commented out when run on cluster.
     print(end-start)
     
     sc.stop()

