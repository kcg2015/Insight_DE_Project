#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 10:34:48 2018

@author: kyleguan
"""

import os
import pyspark
import numpy as np
import time
import datetime
from features import *
from detector import *
from s3_util import *
import helpers
import db_util

if __name__=='__main__': 
     
     aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
     aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

     bucket_name = "test-files-kcguan"
     folder_name_label = "labels/"
     folder_name_image = "images/"
     file_name_label = "test_labels.csv"
     
     model_bucket_name = "models-kcguan"
     model_folder_name = "version-1.0.0/"
     download_path = 'models/' 
     local_path = download_path + model_folder_name
     
     db_name = 'test_result_db'
     tb_name = 'result2'
     model_name = 'model1'
     model_version = '1.0.0'
     
     download_model_files(model_bucket_name, model_folder_name, aws_access_key, aws_secret_access_key,
                         download_path)
     
     image_file_list, bucket = get_file_list(bucket_name, folder_name_image, 
                                             aws_access_key, aws_secret_access_key)
     image_file_list = image_file_list[1:]
     label_dict = read_csv_file(bucket_name, folder_name_label, file_name_label, 
                                aws_access_key, aws_secret_access_key)
     
     
     sc = pyspark.SparkContext()
    
     #Create an RDD from the list of s3 key names to process stored in key_list
     file_list_rdd = sc.parallelize(image_file_list)

     
     boxes = file_list_rdd.map(lambda x: fetch_data(x, bucket)).map(lambda x: det_box(x, local_path))
     
     
     
     db_util.create_database(db_name, tb_name)
     
     start = time.time()
     img_w = 1280
     img_h = 640
     model_name = 'model1'
     model_version = '1.0.0'
     

     #for idx, item in enumerate(boxes.take(len(image_file_list))):
     for idx, item in enumerate(boxes.take(5)):     
          detected_box = item[0] if len(item[0])>0 else []
          detected_box = np.array(detected_box).reshape(-1).tolist()
          
          image_file_name = image_file_list[idx]
          if image_file_name.startswith(folder_name_image):
              image_file_name = image_file_name[len(folder_name_image):]
          
          labeled_box = label_dict[image_file_name]
          iou = helpers.box_iou(detected_box, labeled_box)
          iou = float("{0:.2f}".format(iou))
          conf = float("{0:.2f}".format(item[1]))
          success = str(iou >0.6)
          print(image_file_name, detected_box, labeled_box, iou, conf)
          
          ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
          database_row = [idx, image_file_name, img_w, 
                          img_h, model_name, model_version, 
                          ts, detected_box, labeled_box, 
                          iou, conf, success]
          db_util.insert_database(db_name, tb_name, database_row) 
          
     db_util.print_database(db_name, tb_name)     
     end = time.time()
     
     sc.stop()

