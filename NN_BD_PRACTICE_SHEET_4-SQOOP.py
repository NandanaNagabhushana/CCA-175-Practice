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
 * append mode : ** to be used to sqoop the data that is inserted into the source table( NOT for updated data)
				 Used for importing a table where new rows that are continually being added with increasing row id values. 
                 specify the column containing the row’s id with --check-column. 
				 Sqoop imports rows where the check column has a value greater than the one specified with --last-value.

 * lastmodified : ** to be used when the data is updated (also simultaneously inserted) in the source table - works like upsert/merge
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
  --target-dir /user/dgadiraju/sqoop_import/retail_db/orders \
  --num-mappers 2 \
  --query "select * from orders where \$CONDITIONS and order_date like '2013-%'" \
  --split-by order_id

# Query can be used to load data based on condition - note the usage of append
sqoop import \
  --connect jdbc:mysql://ms.itversity.com:3306/retail_db \
  --username retail_user \
  --password itversity \
  --target-dir /user/dgadiraju/sqoop_import/retail_db/orders \
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
  --target-dir /user/dgadiraju/sqoop_import/retail_db/orders \
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
  --target-dir /user/dgadiraju/sqoop_import/retail_db/orders \
  --num-mappers 2 \
  --table orders \
  --check-column order_date \
  --incremental append \
  --last-value '2014-02-28'
