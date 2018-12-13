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