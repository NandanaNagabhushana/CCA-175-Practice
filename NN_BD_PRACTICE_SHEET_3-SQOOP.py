'''
Sqoop practice
'''

#---------------------
# connect to database
#---------------------

'''
understand the relevance of JAR file
connect string has to be constructed based on the connector jar present in the sqoop libraries

sqoop libraries on itversity lab -->
cd /usr/hdp/current/sqoop-client/lib
ls -ltr

there is a mysql-connector-jaba.jar

conection string : jdbc:mysql://ms.itversity.com:3306
'''

'''
---------------------------------------------------------------
# Sqoop LIST commands : list-databases, list-tables
---------------------------------------------------------------
'''
sqoop help

sqoop list-databases \
--connect jdbc:mysql://ms.itversity.com:3306 \
--username retail_user \
--password itversity

sqoop list-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity


'''
------------------------------------------------------------------------------------------------------------------------------------------------------------
# Sqoop EVAL : (--query) - run various sql queries ; ALL POSSIBLE DDL and DML  --> CRUD and SELECT
--query need to be specified within single/double quotes 
Any valid sql query or command can be run using sqoop eval. 
It is primarily used to load or query log tables while running Sqoop jobs at regular intervals.
------------------------------------------------------------------------------------------------------------------------------------------------------------
'''



sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query 'SELECT * FROM order_items LIMIT 20'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'CREATE TABLE NN_TEST_TAB (col1 INT,col2 VARCHAR(10))'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'insert into NN_TEST_TAB values(1,"NN")'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'insert into NN_TEST_TAB values(2,"ASM")'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'insert into NN_TEST_TAB values(2,"AAN")'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'select * from NN_TEST_TAB LIMIT 20'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'update NN_TEST_TAB set col2 = "AA" where col1 = 1'

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'delete from NN_TEST_TAB where col2 = "XYZ"'


sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'DROP TABLE NN_TEST_TAB'

'''
------------------------------------------------------------------------------------------------------------------------------------------------------------
# Sqoop SIMPLE IMPORT : 

list of control arguments that need to be used for simple sqoop import
	--connect
	--username
	--password
	--table  (source from where the data has to be imported)
	--target-dir OR --warehouse-dir (target to which the data has to be imported into)
	
execution life cycle

num-mappers

	
------------------------------------------------------------------------------------------------------------------------------------------------------------
'''

hadoop fs -mkdir nsqoop_import

hadoop fs -ls /usr/nandanasgn/nsqoop_import/retail_db

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/nandanasgn/nsqoop_import/retail_db

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query 'select count(1) from order_items'

------------------------
| count(1)             |
------------------------
| 172198               |
------------------------

hadoop fs -ls /user/nandanasgn/nsqoop_import
# Found 1 items
# drwxr-xr-x   - nandanasgn hdfs          0 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db

hadoop fs -ls /user/nandanasgn/nsqoop_import/retail_db
'''
Found 5 items
-rw-r--r--   2 nandanasgn hdfs          0 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db/_SUCCESS
-rw-r--r--   2 nandanasgn hdfs    1303818 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db/part-m-00000
-rw-r--r--   2 nandanasgn hdfs    1343222 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db/part-m-00001
-rw-r--r--   2 nandanasgn hdfs    1371917 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db/part-m-00002
-rw-r--r--   2 nandanasgn hdfs    1389923 2018-12-06 02:08 /user/nandanasgn/nsqoop_import/retail_db/part-m-00003
'''

hadoop fs -tail /user/nandanasgn/nsqoop_import/retail_db/part-m-00000

'''
************
??? VIP
************
??? check how to count the number of records in each file -- use shell commands or python
??? verify if all the records have got imported by checking the number of records in the table versus the sum total of number of records in all part-m-* files 
'''


'''
-------------------------------------------------------------------
sqoop command execution life cycle
-------------------------------------------------------------------
1. starts reading data from mysql in streaming fashion
   generates map-reduce code (java)
		understands the structure of data --> 
			extracts the metadata of the table (executes select * from table_name limit 1) - creates table_name.java POJO
			checks the direct path over ride (mysql specific fast path)
			** default num-mappers = 4 
			executes BoundingValQuery - select MIN(PK), MAX(PK) from table_name - uses this info and reduces the big dataset into 4 mutually-exclusive sub-sets on the PK
			
2. compiles the code into a jar
3. submits the jar for execution

'''

# understand the behaviour,structure and size of the data and decide on the num-mappers
# default --num-mappers : 4

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/nandanasgn/nsqoop_import/retail_db \
--num-mappers 1

# ERROR is thrown if the target is already present and NO over-write is specified

#18/12/06 02:37:00 ERROR tool.ImportTool: Encountered IOException running import job: org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://nn01.itversity.com:8020/user/nandanasgn/nsqoop_import/retail_db already exists

# delete if the specified target exists and re-execute the command - SEE only 1 data file - cos 1 mapper is specified

hadoop fs -rm /user/nandanasgn/nsqoop_import/retail_db/part-m*  # removes files with the name specified in the pattern

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/retail_db # recursively removes the directory

# execute import with desired --num-mappers

hadoop fs -ls /user/nandanasgn/nsqoop_import/retail_db
#Found 2 items
#-rw-r--r--   2 nandanasgn hdfs          0 2018-12-06 02:50 /user/nandanasgn/nsqoop_import/retail_db/_SUCCESS
#-rw-r--r--   2 nandanasgn hdfs    5408880 2018-12-06 02:50 /user/nandanasgn/nsqoop_import/retail_db/part-m-00000


'''
-------------------------------------------------------------------
Managing Directories
-------------------------------------------------------------------

understand the difference b/w target-dir and warehouse-dir

usage of --delete-target-dir to delete and recreate the target directory

appending to existing data set -> --append

--append and --delete-target-dir  are mutually EXCLUSIVE

'''

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/retail_db \
--num-mappers 1
--delete-target-dir

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/retail_db \
--num-mappers 1
--append

'''
CUSTOMIZING THE SPLIT-LOGIC 
--spilt-by
 * By default number of mappers is 4, it can be changed with --num-mappers
 * Split logic will be applied on primary key, if exists
 * If primary key does not exists and if we use number of mappers more than 1, then sqoop import will fail
 * At that time we can use --split-by to split on a non key column or explicitly set --num-mappers to 1 or use --auto-reset-to-one-mapper
 * If the primary key column or the column specified in split-by clause is non numeric type, then we need to use this additional argument -Dorg.apache.sqoop.splitter.allow_text_splitter=true
 
 to remember while using --split-by 
 --------------------------
 * For performance reason choose a column which is indexed as part of split-by clause - this way, it will not do a full table scan (will do indexed scan) to get MIN and MAX value in boundingValsQuery
 * If there are null values in the column, corresponding records from the table will be ignored
 * column data should be evenly distributed (sparse) and often should be sequence generated; otherwise (if very random), data might get skewed while making partitions (during mapping) 
 * Data in the split-by column need not be unique, but if there are duplicates then there can be skew in the data while importing (which means some files might be relatively bigger compared to other files)

'''

# order_items_nopk is a table similar to order_items, but doesnt have a primary key
# in the below import, num-mappers are 4 (by default) --> exception gets thrown for NO PK, suggests to use 1 as num-mappers

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items_nopk \
--warehouse-dir /user/dgadiraju/sqoop_import/retail_db

# in such cases, use the split-by on a numeric data column 

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items_nopk \
--warehouse-dir /user/dgadiraju/sqoop_import/retail_db \
--split-by order_item_order_id

#Splitting on text field - fails when split-by is on a non-numeric field, but can exceptionally do it by specifying the control argument -Dorg.apache.sqoop.splitter.allow_text_splitter=true 
# should be specified as the first value prior to all the arguments of sqoop ; OR sqoop.properties should be used to set this property tp true 
sqoop import \
  -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table orders \
  --warehouse-dir /user/dgadiraju/sqoop_import/retail_db \
  --split-by order_status
  
'''
----------------------------
Auto reset to one mapper
----------------------------
 * Some tables might not have primary key
 * If we are not sure whether the table have primary key or not and want to use number of mappers higher than 1, then for those tables where there is no primary key sqoop import will fail
 * One of the way to address this issue is using --auto-reset-to-one-mapper. If there is primary key for the table then sqoop import will use --num-mappers otherwise it will reset mappers to 1.
'''  
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items_nopk \
--warehouse-dir /user/dgadiraju/sqoop_import/retail_db \
--split-by order_item_order_id
--auto-reset-to-one-mapper


'''
-----------------------------------------------------------------------------------------------------------------------------------------------------------
*** File Formats such as text file, sequence file, avro, parquet etc
*** Compression algorithms/codec such as snappy, gzip etc

Sqoop supports below file formats -

	text file (default) --as-textfile       --> plain old text file
	sequence file       --as-sequencefile   --> binary file format
	avro                --as-avrodatafile   --> binary json file format
	parquet             --as-parquetfile    --> binary columnar file format - NPP(Naturally Parallel Processing) databases (green-plum, vertica databases etc.)  
	
-----------------------------------------------------------------------------------------------------------------------------------------------------------
'''

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/parquet_imports/retail_db \
--as-parquetfile

hadoop fs -ls /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items
'''
Found 6 items
drwxr-xr-x   - nandanasgn hdfs          0 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/.metadata
drwxr-xr-x   - nandanasgn hdfs          0 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/.signals
-rw-r--r--   2 nandanasgn hdfs     412162 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/2bd64d5b-0ca0-4736-a48b-d0a43e6744dc.parquet
-rw-r--r--   2 nandanasgn hdfs     411612 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/95a283c2-3511-4bd3-9e50-b23815e4d29b.parquet
-rw-r--r--   2 nandanasgn hdfs     411907 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/b58a03c7-35c5-42f8-a90a-977c150fda82.parquet
-rw-r--r--   2 nandanasgn hdfs     428184 2018-12-06 05:40 /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/e404bd11-aad7-45c1-8598-f021d9659931.parquet

'''

hadoop fs -tail /user/nandanasgn/nsqoop_import/parquet_imports/retail_db/order_items/95a283c2-3511-4bd3-9e50-b23815e4d29b.parquet

# the result looks garbled because it is in Binary Files ; NEVER RUN TAIL or CAT commands in exams because terminals might behave strangely when bashed with special characters

'''
-----------------------------------------------------------------------------------------------------------------------------------------------------------	
Following are the steps to use compression.

	-z, --compress
	--compression-codec <c>

	* Go to /etc/hadoop/conf and check core-site.xml for supported compression codecs
	* Use --compress to enable compression
	* If compression codec is not specified, it will use gzip by default
	* Compression algorithm can be specified using --compression-codec	
			
-----------------------------------------------------------------------------------------------------------------------------------------------------------
'''

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/parquet_imports/*

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/compressed_imports/retail_db \
--as-parquetfile \
--compress

hadoop fs -ls /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items
'''
Found 6 items
drwxr-xr-x   - nandanasgn hdfs          0 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/.metadata
drwxr-xr-x   - nandanasgn hdfs          0 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/.signals
-rw-r--r--   2 nandanasgn hdfs     428184 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/0cc4ba70-6e5f-4182-bcce-313b66c3023d.parquet
-rw-r--r--   2 nandanasgn hdfs     412176 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/35979b06-4995-45c0-a086-7d917c120b04.parquet
-rw-r--r--   2 nandanasgn hdfs     411598 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/6a961963-efea-4ff5-a821-208201d0bc75.parquet
-rw-r--r--   2 nandanasgn hdfs     411921 2018-12-06 06:01 /user/nandanasgn/nsqoop_import/compressed_imports/retail_db/order_items/73943dda-b34a-4a50-89ec-fbc3249643a0.parquet

'''
# ???  check --> not noticing any compression or the change of file extension to gzip when trying to compress the imported parquet file

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db \
--as-textfile \
--compress

hadoop fs -ls /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items
'''
Found 5 items
-rw-r--r--   2 nandanasgn hdfs          0 2018-12-06 06:06 /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items/_SUCCESS
-rw-r--r--   2 nandanasgn hdfs     257743 2018-12-06 06:06 /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items/part-m-00000.gz
-rw-r--r--   2 nandanasgn hdfs     258577 2018-12-06 06:06 /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items/part-m-00001.gz
-rw-r--r--   2 nandanasgn hdfs     259786 2018-12-06 06:06 /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items/part-m-00002.gz
-rw-r--r--   2 nandanasgn hdfs     254614 2018-12-06 06:06 /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items/part-m-00003.gz
'''

# notice the file formats and the sizes of the compressed file fragments

# copy the files from hdfs to local file system
# /home/nandanasgn is the current directory on itversity labs

hadoop fs -get  /user/nandanasgn/nsqoop_import/compressed_imports/text_retail_db/order_items compressed_order_items

ls -ltr compressed_order_items
'''
total 1016
-rw-r--r-- 1 nandanasgn students      0 Dec  6 06:12 _SUCCESS
-rw-r--r-- 1 nandanasgn students 257743 Dec  6 06:12 part-m-00000.gz
-rw-r--r-- 1 nandanasgn students 258577 Dec  6 06:12 part-m-00001.gz
-rw-r--r-- 1 nandanasgn students 259786 Dec  6 06:12 part-m-00002.gz
-rw-r--r-- 1 nandanasgn students 254614 Dec  6 06:12 part-m-00003.gz

'''

gunzip compressed_order_items/part*

ls -ltr compressed_order_ite

'''
total 5288
-rw-r--r-- 1 nandanasgn students       0 Dec  6 06:12 _SUCCESS
-rw-r--r-- 1 nandanasgn students 1303818 Dec  6 06:12 part-m-00000
-rw-r--r-- 1 nandanasgn students 1343222 Dec  6 06:12 part-m-00001
-rw-r--r-- 1 nandanasgn students 1371917 Dec  6 06:12 part-m-00002
-rw-r--r-- 1 nandanasgn students 1389923 Dec  6 06:12 part-m-00003
'''

tail compressed_order_items/part-m-00000
'''
43041,17208,365,4,239.96,59.99
43042,17208,957,1,299.98,299.98
43043,17208,1014,2,99.96,49.98
43044,17209,893,2,49.98,24.99
43045,17209,403,1,129.99,129.99
43046,17209,1014,2,99.96,49.98
43047,17209,502,3,150.0,50.0
43048,17209,977,1,29.99,29.99
43049,17210,502,1,50.0,50.0
43050,17210,567,4,100.0,25.0
'''

# changing the compressing mechanism

 cd /etc/hadoop/conf
 
 ls -ltr
 
 '''
 total 196
-rw-r--r-- 1 root   root    2250 Aug 25  2016 yarn-env.cmd
-rw-r--r-- 1 mapred hadoop  2697 Aug 25  2016 ssl-server.xml.example
-rw-r--r-- 1 mapred hadoop  2316 Aug 25  2016 ssl-client.xml.example
-rw-r--r-- 1 root   root     758 Aug 25  2016 mapred-site.xml.template
-rw-r--r-- 1 root   root    4113 Aug 25  2016 mapred-queues.xml.template
-rw-r--r-- 1 root   root     951 Aug 25  2016 mapred-env.cmd
-rw-r--r-- 1 root   root    5511 Aug 25  2016 kms-site.xml
-rw-r--r-- 1 root   root    1631 Aug 25  2016 kms-log4j.properties
-rw-r--r-- 1 root   root    1527 Aug 25  2016 kms-env.sh
-rw-r--r-- 1 root   root    3518 Aug 25  2016 kms-acls.xml
-rw-r--r-- 1 root   root    2490 Aug 25  2016 hadoop-metrics.properties
-rw-r--r-- 1 root   root    3979 Aug 25  2016 hadoop-env.cmd
-rw-r--r-- 1 hdfs   hadoop  1335 Aug 25  2016 configuration.xsl
-rw-r--r-- 1 hdfs   hadoop  1308 Jun  5  2017 hadoop-policy.xml
-rw-r--r-- 1 hdfs   hadoop   884 Jun  5  2017 ssl-client.xml
drwxr-xr-x 2 root   hadoop  4096 Jun  5  2017 secure
-rw-r--r-- 1 hdfs   hadoop  1000 Jun  5  2017 ssl-server.xml
-rw-r--r-- 1 hdfs   hadoop  2135 Jun  5  2017 capacity-scheduler.xml
-rw-r--r-- 1 root   hadoop  1079 Jun  5  2017 container-executor.cfg
-rw-r--r-- 1 hdfs   root     945 Jun  5  2017 taskcontroller.cfg
-rwxr-xr-x 1 hdfs   root     856 Jun  5  2017 mapred-env.sh
-rw-r--r-- 1 hdfs   hadoop 10411 Jun  5  2017 log4j.properties
-rw-r--r-- 1 hdfs   root    1602 Jun  5  2017 health_check
-rw-r--r-- 1 hdfs   root    1020 Jun  5  2017 commons-logging.properties
-rwxr-xr-x 1 root   root    4221 Jun  5  2017 task-log4j.properties
-rwxr-xr-x 1 root   root    2358 Jun  5  2017 topology_script.py
-rw-r--r-- 1 mapred hadoop  7019 Aug  3 15:44 mapred-site.xml
-rwxr-xr-x 1 yarn   hadoop  5293 Aug  3 15:44 yarn-env.sh
-rw-r--r-- 1 hdfs   hadoop  5667 Aug  3 23:23 hadoop-env.sh
-rw-r--r-- 1 hdfs   hadoop  2263 Aug  3 23:23 hadoop-metrics2.properties
-rw-r--r-- 1 hdfs   root      96 Oct 20 13:51 slaves
-rw-r--r-- 1 hdfs   hadoop   319 Oct 20 13:53 topology_mappings.data
-rw-r--r-- 1 yarn   hadoop 18964 Oct 20 13:53 yarn-site.xml
-rw-r--r-- 1 hdfs   hadoop  6531 Oct 25 01:13 hdfs-site.xml
-rw-r--r-- 1 hdfs   hadoop  4130 Nov 16 22:34 core-site.xml
'''

# 2 config files to be considered here  --> core-site.xml hdfs-site.xml

vi core-site.xml

# search for codec using /codec ; all enabled compression codecs will be listed
# here there are 3 --> GZIP : org.apache.hadoop.io.compress.GzipCodec
#					   default hdfs codec : org.apache.hadoop.io.compress.DefaultCodec
#					   SNAPPY : org.apache.hadoop.io.compress.SnappyCodec 	
'''
    <property>
      <name>io.compression.codecs</name>
      <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec</value>
    </property>
'''
# give the fully qualified class name for the codec in --compression-codec

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db \
--as-textfile \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec

hadoop fs -ls /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items
'''
Found 5 items
-rw-r--r--   2 nandanasgn hdfs          0 2018-12-06 06:29 /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items/_SUCCESS
-rw-r--r--   2 nandanasgn hdfs     456557 2018-12-06 06:29 /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items/part-m-00000.snappy
-rw-r--r--   2 nandanasgn hdfs     459317 2018-12-06 06:29 /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items/part-m-00001.snappy
-rw-r--r--   2 nandanasgn hdfs     458768 2018-12-06 06:29 /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items/part-m-00002.snappy
-rw-r--r--   2 nandanasgn hdfs     450824 2018-12-06 06:29 /user/nandanasgn/nsqoop_import/compressed_imports/snappy_retail_db/order_items/part-m-00003.snappy
'''

'''
--------------------------------------------------------------------------------------------------------------------------------------------------------------
FILTERING DATA DURING SQOOP IMPORT

While performing sqoop import data can be filtered by -
	Using --boundary-query
	Specifying Columns --columns
	Using --query

Boundary Query
---------------
Let us see the purpose of --boundary-query
	Let us recap typical import life cycle
		* Using primary key field get min and max values (boundary query)
		* Compute ranges using number of mappers (splits)
		* Establish database connection for each split and issue query to read the data
		* Get data and write it o HDFS
	Using --boundary-query
		We can avoid issuing query to get min and max values by hard coding them (if we know the values up front)
		We can address the issue of outliers by narrowing down using where clause in --boundary-query
--------------------------------------------------------------------------------------------------------------------------------------------------------------
'''

hadoop fs -rm -R /user/nandanasgn/nsqoop_import/retail_db/*

sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --table order_items \
  --warehouse-dir /user/nandanasgn/nsqoop_import/retail_db \
  --boundary-query 'select 100000, 172198'
  
sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--query 'select min(order_item_id), min(order_item_id) from order_items group by order_item_order_id having sum(order_item_subtotal) > 300'
  
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--warehouse-dir user/nandanasgn/nsqoop_import/retail_db \
--boundary-query 'select min(order_item_id), min(order_item_id) from order_items group by order_item_order_id having sum(order_item_subtotal) > 300'
'''
18/12/07 04:15:30 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_id), min(order_item_id) from order_items group by order_item_order_id having sum(order_item_subtotal) > 300
...
...
18/12/07 04:15:47 INFO mapreduce.ImportJobBase: Transferred 25 bytes in 25.5958 seconds (0.9767 bytes/sec)
18/12/07 04:15:47 INFO mapreduce.ImportJobBase: Retrieved 1 records.
'''

#select min(order_item_id), min(order_item_id),order_item_order_id from order_items group by order_item_order_id having sum(order_item_subtotal) > 1000

#select * from order_items where order_item_order_id =  68703 order by order_item_id


'''
--------------------------------------------------------------------------------------------------------------------------------------------------------------
TRANSFORMING DATA DURING SQOOP IMPORT

We can also apply some basic transformations as part of Sqoop Import

 * Specify columns we want to extract using --columns
 * We can pass custom query instead of table using --query
 * When --query is used we need to specify --split-by or set --num-mappers to 1; we cannot set num-mappers more than 1 if we dont specify the --split-by

-----------------------
* THINGS TO REMEMBER *
-----------------------
 * --table and --query are MUTUALLY EXCLUSIVE
 * --columns and --query are MUTUALLY EXCLUSIVE
 * while using --columns --> the column names should be comma seperated, and should not have any other characters(not even space) in the comma seperated list
 * --columns is mostly used for vertical slicing of the table; if --split-by is unspecified, the partition will still happen on the table-defined PK; complete metadata of the table (all columns) is collected during creation of the table POJO jar. 
 * --query is often used to import data that is the resultant of join b/w two or more tables , and/or has transformations on the columns (eg. sum(col1), col1||col2 etc.)
 * when --query is used, --warehouse-dir cannot be used, because --table is not being specified anymore, and the engine will not understand the directory into which the file as to be placed . Therefore, --target-dir should be used with --query
 * when using --query, "and \$CONDITIONS" has to be appended to where clause. This is only for syntactical correctness.

--------------------------------------------------------------------------------------------------------------------------------------------------------------
'''

hadoop fs -rm -R /user/nandanasgn/user/nandanasgn/nsqoop_import/retail_db/*

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--columns order_item_order_id,order_item_id,order_item_quantity \
--warehouse-dir user/nandanasgn/nsqoop_import/retail_db

 hadoop fs -rm -R /user/nandanasgn/user/nandanasgn/nsqoop_import/retail_db/*
 
 
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/dgadiraju/sqoop_import/retail_db/orders_with_revenue \
  --num-mappers 2 \
  --query 'select o.*, sum(oi.order_item_subtotal) order_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id and \$CONDITIONS group by o.order_id, o.order_date, o.order_customer_id, o.order_status' \
  --split-by order_id
 
 
 '''
 Let us get into controlling the imports

Specifying delimiters - 
	--enclosed-by <char>			Sets a required field enclosing character
	--escaped-by <char>				Sets the escape character
	--fields-terminated-by <char>	Sets the field separator character
	--lines-terminated-by <char>	Sets the end-of-line character
	--optionally-enclosed-by <char>	Sets a field enclosing character
	--mysql-delimiters	            Uses MySQL’s default delimiter set: 
											fields: , 
											lines: \n 
											escaped-by: \ 
											optionally-enclosed-by: '	

Handling nulls - 
	--null-string <null-string>      The string to be written for a null value for string columns
	--null-non-string <null-string>  The string to be written for a null value for non-string column


'''
hadoop fs -rm -R /user/nandanasgn/nsqoop_import/hr_db

#Default behavior
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/hr_db \
  --username hr_user \
  --password itversity \
  --table employees \
  --warehouse-dir /user/nandanasgn/nsqoop_import/hr_db
  
hadoop fs -ls /user/nandanasgn/nsqoop_import/hr_db/*  

hadoop fs -tail /user/nandanasgn/nsqoop_import/hr_db/employees/part-m-00000

hadoop fs -get /user/nandanasgn/nsqoop_import/hr_db/employees #getting/copying the files from 
#hdfs to local file system; here the target is current directory  

hadoop fs -get /user/nandanasgn/nsqoop_import/hr_db/employees /home/nandanasgn/nsqoop_import

view /home/nandanasgn/nsqoop_import/employees/part-m-00000
  
#Changing default delimiters and nulls
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/hr_db \
  --username hr_user \
  --password itversity \
  --table employees \
  --warehouse-dir /user/nandanasgn/nsqoop_import/hr_db_delimiters \
  --null-non-string -1 \
  --fields-terminated-by "\000" \
  --lines-terminated-by ":"
  
hadoop fs -ls /user/nandanasgn/nsqoop_import/hr_db_delimiters/*  

hadoop fs -get /user/nandanasgn/nsqoop_import/hr_db_delimiters /home/nandanasgn/nsqoop_import

ls -ltr /home/nandanasgn/nsqoop_import/hr_db_delimiters

view /home/nandanasgn/nsqoop_import/hr_db_delimiters/employees/part-m-00000
