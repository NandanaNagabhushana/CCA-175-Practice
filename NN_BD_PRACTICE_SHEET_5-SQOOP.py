'''
-------------------------------------------------------------------------------------------------
				              SQOOP EXPORT
-------------------------------------------------------------------------------------------------

Typical life cycle of data-processing 

 1. Data ingestion into HDFS : (sqoop as one of the methods)
       Get data ingested to HDFS using Sqoop import from relational databases
	   
 2. Process Data : 	   
		Process data using Map Reduce or Spark
		
 3. Visualize processed data
	* connect BI/Visualization tools to HDFS directly (OR)
	* Processed data can be exported back to databases supporting reporting layer
	
	
SQOOP EXPORT is the tool to export data from hdfs to databases	

'''


sqoop import \
--connect jdbc:mysql://ms.itversity.com:3306/retail_db \
--username retail_user \
--password itversity \
--table orders \
--target-dir /user/nandanasgn/nsqoop_import/hive_retail_db/orders \
--hive-import \
--hive-overwrite \
--hive-database nsgn_hive_db \
--hive-table orders \
--num-mappers 2


create table daily_revenue as 
select o.order_date,sum(oi.order_item_subtotal) daily_revenue
from orders o, order_items oi
where o.order_id = oi.order_item_order_id
group by o.order_date

create table daily_revenue_jul_2013 as 
select o.order_date,sum(oi.order_item_subtotal) daily_revenue
from orders o, order_items oi
where o.order_id = oi.order_item_order_id
and o.order_date like '2013-07%'
group by o.order_date

hive (nsgn_hive_db)> describe formatted daily_revenue_jul_2013;
'''
OK
# col_name              data_type               comment

order_date              string
daily_revenue           double

# Detailed Table Information
Database:               nsgn_hive_db
Owner:                  nandanasgn
CreateTime:             Fri Dec 14 05:59:38 EST 2018
LastAccessTime:         UNKNOWN
Protect Mode:           None
Retention:              0
Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/daily_revenue_jul_2013
Table Type:             MANAGED_TABLE
Table Parameters:
        COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\"}
        numFiles                1
        numRows                 7
        rawDataSize             277
        totalSize               284
        transient_lastDdlTime   1544785178

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
Time taken: 0.401 seconds, Fetched: 32 row(s)
'''



'''
--------------------------------------------------------------------------------------------
Sqoop Export – Simple Export
--------------------------------------------------------------------------------------------

As part of this topic we will run a simple export with delimiters

Simple export – following are the arguments we need to pass
	--connect with jdbc connect string. It should include target database
	--username and --password, the user should have right permission on the table into which data is being exported
	--table, target table in relational database such as MySQL into which data need to be copied
	--export-dir source hdfs location from where data need to be exported into the dbms table
Delimiters
	Sqoop by default expect “,” to be field delimiter
	But Hive default delimiter is Ascii 1 (\001)
--input-fields-terminated-by can be used to pass delimiting character other than ","

Number of mappers – we can increase or decrease number of threads by using --num-mappers or -m

'''


#Create a table in MySql
#Use database retail_export if you want to create the tables and export the data

create table daily_revenue(
  order_date varchar(30),
  revenue float
);

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'create table nsgn_daily_revenue(order_date varchar(30),revenue float)'


sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'describe nsgn_daily_revenue'
'''

18/12/14 06:08:43 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
---------------------------------------------------------------------------------------------------------
| Field                | Type                 | Null | Key | Default              | Extra                |
---------------------------------------------------------------------------------------------------------
| order_date           | varchar(30)          | YES |     | (null)               |                      |
| revenue              | float                | YES |     | (null)               |                      |
---------------------------------------------------------------------------------------------------------
'''

# OR use SQOOP EVAL to create the table

#Sqoop Exporting the Data ; note the field-delimiter specification --> should be inline with the delimiters used while creating the hive-table-data (HDFS data) 
# ctrl+A = \001
# tab = \t
 
sqoop export \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--export-dir hdfs://nn01.itversity.com:8020/apps/hive/warehouse/nsgn_hive_db.db/daily_revenue_jul_2013 \
--table nsgn_daily_revenue \
--input-fields-terminated-by "\001" 
 
'''
18/12/14 06:45:02 INFO mapreduce.ExportJobBase: Transferred 1.4043 KB in 26.4886 seconds (54.2875 bytes/sec)
18/12/14 06:45:02 INFO mapreduce.ExportJobBase: Exported 7 records.
'''

sqoop eval \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--query 'select * from nsgn_daily_revenue' 
'''
18/12/14 06:49:26 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6.2.6.5.0-292
18/12/14 06:49:26 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
18/12/14 06:49:26 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
---------------------------------------
| order_date           | revenue      |
---------------------------------------
| 2013-07-31 00:00:00.0 | 131878       |
| 2013-07-25 00:00:00.0 | 68153.8      |
| 2013-07-26 00:00:00.0 | 136520       |
| 2013-07-27 00:00:00.0 | 101074       |
| 2013-07-28 00:00:00.0 | 87123.1      |
| 2013-07-29 00:00:00.0 | 137287       |
| 2013-07-30 00:00:00.0 | 102746       |
---------------------------------------
'''


 
 '''
 Sqoop Export Behavior
Read data from export directory
By default, Sqoop export uses 4 parallel threads to read the data by using Map Reduce split logic (based up on HDFS block size)
Each thread establishes database connection using JDBC url, username and password
Generated insert statement to load data into target table
Issues insert statements in the target table using connection established per thread (or mapper)


Export control arguments:
---------------------------------------------------------------------------------------------------------
Argument											Description
---------------------------------------------------------------------------------------------------------
--columns <col,col,col…>				|	Columns to export to table
--direct								|	Use direct export fast path
--export-dir <dir>						|	HDFS source path for the export
-m,--num-mappers <n>					|	Use n map tasks to export in parallel
--table <table-name>					|	Table to populate
--call <stored-proc-name>				|	Stored Procedure to call
--update-key <col-name>					|	Anchor column to use for updates. Use a comma separated list of columns if there are more than one column.
--update-mode <mode>					|	Specify how updates are performed when new rows are found with non-matching keys in database.
Legal values for mode include updateonly(default) and allowinsert.
--input-null-string <null-string>		|	The string to be interpreted as null for string columns
--input-null-non-string <null-string>	|	The string to be interpreted as null for non-string columns
--staging-table <staging-table-name>	|	The table in which data will be staged before being inserted into the destination table.
--clear-staging-table					|	Indicates that any data present in the staging table can be deleted.
--batch									|	Use batch mode for underlying statement execution.
---------------------------------------------------------------------------------------------------------                                       
                                        
Table 30. Input parsing arguments:

---------------------------------------------------------------------------------------------------------
Argument											Description
---------------------------------------------------------------------------------------------------------
--input-enclosed-by <char>				|	Sets a required field encloser
--input-escaped-by <char>				|	Sets the input escape character
--input-fields-terminated-by <char>		|		Sets the input field separator
--input-lines-terminated-by <char>		|	Sets the input end-of-line character
--input-optionally-enclosed-by <char>	|	Sets a field enclosing character
                                        |
---------------------------------------------------------------------------------------------------------										
										
Table 31. Output line formatting arguments:
                                        
---------------------------------------------------------------------------------------------------------
Argument											Description
---------------------------------------------------------------------------------------------------------                 
--enclosed-by <char>					|	Sets a required field enclosing character
--escaped-by <char>						|	Sets the escape character
--fields-terminated-by <char>			|	Sets the field separator character
--lines-terminated-by <char>			|	Sets the end-of-line character
--mysql-delimiters						|	ses MySQL’s default delimiter set: fields: , lines: \n escaped-by: \ optionally-enclosed-by: '
--optionally-enclosed-by <char>			|	Sets a field enclosing character
--------------------------------------------------------------------------------------------------------- 										
										
										
'''  


 
'''                                   
Column Mapping                           
Let us see rationale behind column mappi ng while exporting the data
                                         
Some times the structure of data in HDFS and structure of table in MySQL into which data need to be exported need not match exactly
There is no way we can change the order of columns in our input data and we have to consume every column
However, Sqoop export give flexibility to map all the columns to target table columns in the order of data in HDFS. For e.g.
HDFS data structure – order_date and revenue
MySQL target table – revenue, order_date and description
There is no description in HDFS and hence description in target table should be nullable
--columns order_date,revenue will make sure data is populated into revenue and order_date in target table.

'''


create table daily_revenue_demo (
     revenue float,
     order_date varchar(30),
     description varchar(200)
);


sqoop export \
--connect jdbc:mysql://ms.itversity.com:3306/retail_export \
--username retail_user \
--password itversity \
--export-dir /apps/hive/warehouse/dgadiraju_sqoop_import.db/daily_revenue \
--table daily_revenue_demo \
--columns order_date,revenue \
--input-fields-terminated-by "\001" \
--num-mappers 1
 
