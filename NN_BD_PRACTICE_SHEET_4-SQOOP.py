'''
sqoop incremental imports

Incremental loads can be performed using

 * always --append should be used for incremental uploads/imports

query
where
Standard incremental loads - 
	Sqoop provides an incremental import mode which can be used to retrieve only rows newer than some previously-imported set of rows.
	The following arguments control incremental imports:
		--check-column (col)	Specifies the column to be examined when determining which rows to import. (the column should not be of type CHAR/NCHAR/VARCHAR/VARNCHAR/ LONGVARCHAR/LONGNVARCHAR)
		--incremental (mode)	Specifies how Sqoop determines which rows are new. Legal values for mode include append and lastmodified.
		--last-value (value)	Specifies the maximum value of the check column from the previous import.
		

 * Sqoop supports two types of incremental imports: 
			1. append   
			2. lastmodified 
 * --incremental argument should be used to specify the type of incremental import to perform.
 * append mode : ** typically used to sqoop the data from isert-only source tables( NOT for updated data), and is generally done using the PK of the table
				 Used for importing a table where new rows that are continually being added with increasing row id values. 
                 specify the column containing the row’s id with --check-column. 
				 Sqoop imports rows where the check column has a value greater than the one specified with --last-value.

 * lastmodified : ** to be used when the data is updated (also simultaneously inserted) in the source table - works like upsert/merge; generally a date/timestamp is used to specify the check-column
				  An alternate table update strategy, should use this when rows of the source table may be updated, 
                  and each such update will set the value of a last-modified column to the current timestamp. 
				  Rows where the check column holds a timestamp more recent than the timestamp specified with --last-value are imported.

*** P.s. ***
At the end of an incremental import, the value which should be specified as --last-value for a subsequent import is printed to the screen. 
When running a subsequent import, you should specify --last-value in this way to ensure you import only the new or updated data. 
This is handled automatically by creating an incremental import as a saved job, which is the preferred mechanism for performing a recurring incremental import.
		

'''

# Baseline import
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/nandanasgn/nsqoop_import/retail_db/orders \
  --num-mappers 2 \
  --query "select * from orders where \$CONDITIONS and order_date like '2013-%'" \
  --split-by order_id

# Query can be used to load data based on condition - note the usage of append
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/nandanasgn/nsqoop_import/retail_db/orders \
  --num-mappers 2 \
  --query "select * from orders where \$CONDITIONS and order_date like '2014-01%'" \
  --split-by order_id \
  --append
  
# the drawback of --query is that --split-by should be specified which might get complicated with the increase
# in data size and complexity of data structure  

# --where in conjunction with --table can be used to get data based up on a condition
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/nandanasgn/nsqoop_import/retail_db/orders \
  --num-mappers 2 \
  --table orders \
  --where "order_date like '2014-02%'" \
  --append
  
# still there is a drawback - to understand from where the increment has to be performed, --eval will have to be used
# both prior and post the imports; to overcome this, the standard incremental imports should be used   

# Incremental load using arguments specific to incremental load
  
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--target-dir /user/nandanasgn/nsqoop_import/retail_db/orders \
--num-mappers 2 \
--table orders \
--check-column order-date \
--incremental append \
--last-value '2014-02-28'


# P.S. --> learn to use sqoop job; work more on developing a sqoop solution for incremental uploads from mysql to hdfs

'''
SQOOP HIVE IMPORTS
----------------------------------------------------------------------------------------------------------------------------------------	
    Argument						Description
----------------------------------------------------------------------------------------------------------------------------------------	
--hive-home <dir>				Override $HIVE_HOME
--hive-import					Import tables into Hive (Uses Hive’s default delimiters if none are set.)
--hive-overwrite				Overwrite existing data in the Hive table.
--create-hive-table				If set, then the job will fail if the target hive table exits. By default this property is false.
--hive-table <table-name>		Sets the table name to use when importing to Hive.
--hive-database 				(NOT DOCUMENTED) Specify the Hive database onto which the table has to be created/loaded. If this is not specified, then db_name.table_name has to be specified in --hive-table
--hive-drop-import-delims		Drops \n, \r, and \01 from string fields when importing to Hive.
--hive-delims-replacement		Replace \n, \r, and \01 from string fields with user defined string when importing to Hive.
--hive-partition-key			Name of a hive field to partition are sharded on
--hive-partition-value <v>		String-value that serves as partition key for this imported into hive in this job.
--map-column-hive <map>			Override default mapping from SQL type to Hive type for configured columns.



Sqoop provide several features to get data into Hive tables

	Create empty hive table
	Create new hive table and load into it
	Copy data into existing hive table
	and more

Create Database
Before getting into Hive import, it is better to create a Hive database to explore the features of Sqoop import into Hive.

Simple Hive Import

--hive-import will enable hive import. It create table if it does not already exists
--hive-database can be used to specify the database
Instead of --hive-database, we can use database name as prefix as part of --hive-table


Managing Tables - 3 approaches
 (1)Default hive import behavior - append data 
		- Create table if table does not exists
		- If table already exists, data will be appended
  (2) overwrite  ->  --hive-overwrite will replace existing data with new set of data
  (3) --create-hive-table will fail hive import, if table already exists
	
	*** NOTE ***  --hive-overwrite and  --create-hive-table should be MUTUALLY EXCLUSIVELY used

'''

>>> hive

create database nsgn_hive_db;
use nsgn_hive_db;
show tables;
create table nn(col1 int);
insert into nn values(11101985);

select * from nn;
'''
OK
11101985
Time taken: 0.231 seconds, Fetched: 1 row(s)
'''

describe nn;
'''
OK
col1                    int
Time taken: 0.355 seconds, Fetched: 1 row(s)
'''

describe formatted nn;
'''
OK
# col_name              data_type               comment

col1                    int

# Detailed Table Information
Database:               nsgn_hive_db
Owner:                  nandanasgn
CreateTime:             Thu Dec 13 06:18:50 EST 2018
LastAccessTime:         UNKNOWN
Protect Mode:           None
Retention:              0
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/nn
Table Type:             MANAGED_TABLE
Table Parameters:
        COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\"}
        numFiles                1
        numRows                 1
        rawDataSize             8
        totalSize               9
        transient_lastDdlTime   1544699959

# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Compressed:             No
Num Buckets:            -1
Bucket Columns:         []
Sort Columns:           []
Storage Desc Params:
        serialization.format    1
Time taken: 0.355 seconds, Fetched: 31 row(s)
'''
  
sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--hive-import \
--hive-database nsgn_hive_db \
--hive-table order_items \
--num-mappers 2
'''
18/12/13 06:32:59 ERROR tool.ImportTool: Encountered IOException running import job: org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://nn01.itversity.com:8020/user/nandanasgn/order_items already exists
'''

# if a HDFS location is already present, an exception is thrown. To avoid this, --target-dir or --warehouse-dir should be specified

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/nandanasgn/nsqoop_import/hive_retail_db/order_items \
--hive-import \
--hive-database nsgn_hive_db \
--hive-table order_items \
--num-mappers 2

'''
...
18/12/13 06:37:19 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-nandanasgn/compile/17ab8462f626d694c4e46fb8f95f7c71/order_items.jar
...
18/12/13 06:37:29 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_item_id`), MAX(`order_item_id`) FROM `order_items`
18/12/13 06:37:29 INFO db.IntegerSplitter: Split size: 86098; Num splits: 2 from: 1 to: 172198
18/12/13 06:37:30 INFO mapreduce.JobSubmitter: number of splits:2
...
Logging initialized using configuration in jar:file:/usr/hdp/2.6.5.0-292/hive/lib/hive-common-1.2.1000.2.6.5.0-292.jar!/hive-log4j.properties
OK
Time taken: 2.67 seconds
Loading data to table nsgn_hive_db.order_items
Table nsgn_hive_db.order_items stats: [numFiles=2, numRows=0, totalSize=5408880, rawDataSize=0]
OK
Time taken: 0.903 seconds
'''

# note : data will be loaded onto hive table only after a HDFS import happens --> in other words, the data placed in HDFS is represented in a structured format in the HIVE. By creating a hive database, and a table in it, we will be creating a structure and pointers to the data stored in HDFS.

hadoop fs -ls /user/nandanasgn/nsqoop_import/hive_retail_db 
# this actually gives out nothing because this is the path given (in --target-dir) to instruct where the temp files need to be written. If the sqoop import is successful, the directory will be emptied, otherwise the temp files would still exist here, and an exception will be thrown out stating the same. therefore, it is safe to clear the temp directory prior to hive-import

# TO KNOW WHERE THE TABLE'S UNDERLYING DATA IS STORED IN HDFS , "describe formatted ORDER_ITEMS" should be used
hive (nsgn_hive_db)> describe formatted order_items;
'''
OK
# col_name              data_type               comment

order_item_id           int
order_item_order_id     int
order_item_product_id   int
order_item_quantity     tinyint
order_item_subtotal     double
order_item_product_price        double

# Detailed Table Information
Database:               nsgn_hive_db
Owner:                  nandanasgn
CreateTime:             Thu Dec 13 06:37:53 EST 2018
LastAccessTime:         UNKNOWN
Protect Mode:           None
Retention:              0
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items
Table Type:             MANAGED_TABLE
Table Parameters:
        comment                 Imported by sqoop on 2018/12/13 06:37:48
        numFiles                2
        numRows                 0
        rawDataSize             0
        totalSize               5408880
        transient_lastDdlTime   1544701074

# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Compressed:             No
Num Buckets:            -1
Bucket Columns:         []
Sort Columns:           []
Storage Desc Params:
        field.delim             \u0001
        line.delim              \n
        serialization.format    \u0001
Time taken: 0.35 seconds, Fetched: 38 row(s)
'''
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items
'''
Found 2 items
-rwxrwxrwx   2 nandanasgn hdfs    2647040 2018-12-13 06:37 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00000
-rwxrwxrwx   2 nandanasgn hdfs    2761840 2018-12-13 06:37 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00001
'''

hadoop fs -get hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items /home/nandanasgn/hive_db_order_items

ls -ltr /home/nandanasgn/hive_db_order_items
'''
total 5288
-rw-r--r-- 1 nandanasgn students 2647040 Dec 13 06:53 part-m-00000
-rw-r--r-- 1 nandanasgn students 2761840 Dec 13 06:53 part-m-00001
'''
view /home/nandanasgn/hive_db_order_items/part-m-00000

# *** NOTE *** the deafult field delimiter in hdfs is ",", but in hive it is "^A" (ctrl+a)
'''
hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items
Found 4 items
-rwxrwxrwx   2 nandanasgn hdfs    2647040 2018-12-13 06:37 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00000
-rwxrwxrwx   2 nandanasgn hdfs    2647040 2018-12-13 07:04 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00000_copy_1
-rwxrwxrwx   2 nandanasgn hdfs    2761840 2018-12-13 06:37 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00001
-rwxrwxrwx   2 nandanasgn hdfs    2761840 2018-12-13 07:04 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00001_copy_1

'''
# by default, hive-import appends to the table
# to specify overwrite -->  --hive-overwrite

sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table order_items \
--target-dir /user/nandanasgn/nsqoop_import/hive_retail_db/order_items \
--hive-import \
--hive-overwrite \
--hive-database nsgn_hive_db \
--hive-table order_items \
--num-mappers 2

hadoop fs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items
'''
Found 3 items
-rwxrwxrwx   2 nandanasgn hdfs          0 2018-12-13 07:08 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/_SUCCESS
-rwxrwxrwx   2 nandanasgn hdfs    2647040 2018-12-13 07:08 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00000
-rwxrwxrwx   2 nandanasgn hdfs    2761840 2018-12-13 07:08 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/order_items/part-m-00001
'''

'''
-------------------------------------------------------------------------------------------------
				HIVE --IMPORT-ALL--TABLES
-------------------------------------------------------------------------------------------------

Sqoop provides capability to import all the tables using import-all-tables

All the tables from a schema/database can be imported
--exclude-tables : will facilitate to exclude the tables that need not be imported
--autoreset-to-one-mapper, will let import all tables to choose one mapper in case table does not have primary key
Most of the features such as --query, --boundary-query, --where, --split-by etc. are not available with import-all-tables.


limitations - 
	* --warehouse-dir is mandatory
	* good practice to use --autoreset-to-one-mapper to avoid failures upon importing tables without pk/indexed column
	* filtering or transformation of data is not possible --> cannot specify --cols ,--query, --where, --split-by etc.
	* incremental imports are not possible with import-all-tables; this has to be used to perform the initial base import, and later tables need be be individually incrementally imported

'''

sqoop import-all-tables \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--warehouse-dir /user/nandanasgn/nsqoop_import/import_all_retail_db \
--autoreset-to-one-mapper


hadoop fs -ls /user/nandanasgn/nsqoop_import/import_all_retail_db
