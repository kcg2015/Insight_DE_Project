# Scalable ML Model Validation


In this project, I build a automated pipeline to help data scientists and engineers stress-test mission-critical machine learning models over hundreds of million of images/samples/scenarios. Specififically, I use a Tensorflow based  traffic light detector as an example.

The pipeline consists of the following three stages in tandem:

1) Data storage:  I use Amazon Web Service (AWS) S3 services

2) Distributed computing/model validation: I use a Apache Spark cluster that consits of one master and three workers.

3) Database: I use PostgreSQL to build a relational database

### Dependencies

* Java 8 + OpenJDK
* Zookeeper 3.4.9
* Kafka 0.10.1.1
* Hadoop 2.7.4
* Spark 2.1.1
* pyspark 2.1.1+hadoop2.7
* OpenCV 3.1.0
* TensorFlow 1.2.0 and 1.8.0
* boto 2.48.0
* PostgreSQL 9.5.13
* psycopg2 2.7.5

###  Installation
```
git clone https://github.com/kcg2015/Insight_DE_Project.git
```
### Running the scripts

#### Create a database

```
sudo -u postgres psql
sudo -u postgres createdb -O data_engineer test_result_db
```
#### Spark batch processing

```
bin/spark-submit --master spark://ip-10-0-0-9:7077 \
--conf "spark.executor.memory=5g" \
--py-files /home/ubuntu/Insight_DE_project/tl_detector.py,/home/ubuntu/Insight_DE_project/s3_util.py,/home/ubuntu/Insight_DE_project/db_util.py \
/home/ubuntu/Insight_DE_projcet/main.py
```




 


