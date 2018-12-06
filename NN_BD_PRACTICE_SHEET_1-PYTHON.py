# NN's practice
'''
initialize a pyspark job using pyspark console
1. Run in yarn mode; control arguments; decide on the number of executors; set-up additional properties if required;
'''
pyspark --master yarn --conf spark.ui.port=65534 #default port is 4040 should be a 5 digit number less than 65535

'''
SPARK programmamtic initialization of a job
1. create a configuration object
2. create sparkContext object
'''


'''
connect mySql
'''
mysql -u retail_dba -h nn01.itversity.com -p


# read data from hdfs and create RDD
# textfile, hadoopFile, newAPIHadoopFile, sequenceFile

hadoop fs -ls /public/retail_db # hdfs location for retail_db tables

oi_rdd = sc.textFile("/public/retail_db/order_items")

for i in oi_rdd.map(lambda oi:oi.split(",")[3]).take(10): \
	print(i)

	
oi_rdd.toDebugString()	# gives out the details about oi_rdd DAG

# actions to preview data
# collect(returns a python list), count, first, take, takeSample, takeOrdered

# ??????????????????????????????
# container database
# pluggable database
# ??????????????????????????????

l = range(1,1000)
type(l)

l_rdd = sc.parallelize(l)



http://discuss.itversity.com/t/spark-machine-learning-let-us-learn-machine-learning-using-yelp-dataset/13840


https://intellipaat.com/tutorial/spark-tutorial/



 products_raw = open("/data/retail_db/products/part-00000").read().splitlines()  # reading from an external file location into a python collection
 type(products_raw)
 pr_rdd = sc.parallelize(products_raw)
 
 
 
 
 # load() and read() -->json,parquet,orc,text can be used to read data of different file formats into an RDD
 
 help(sqlContext.read.json)  --> has metadata associated, therefore table structure not required
 help(sqlContext.read.orc)   --> has metadata associated, therefore table structure not required
 help(sqlContext.read.parquet)  --> has metadata associated, therefore table structure not required
 help(sqlContext.read.text)   --> does not have metadata associated
   
 
orders = sc.textFile("/public/retail_db/orders")
 type(orders)
 
s = orders.first()
type(s)
len(s)
s[2:10]
s.split(",")[1]
s.split(",")[1].split(" ")[0]
another_str = s.replace('xx',' ')

'''
get top N products by Price within each product-category  - By Key ranking

groupByKey + flatMap
'''

products = sc.textFile("/public/retail_db/products")
productsFiltered = products.filter(lambda p: p.split(",")[3] != "") #to remove records with null in a particular field
for i in productsFiltered.take(5): print(i)

productsFiltered.count()
products.count()

productsMap = productsFiltered.map(lambda p: (int(p.split(",")[1]),p)) #tuple'fying the records as (k,v) where p[1] is the category-id
type(productsMap)
for i in productsMap.take(5): print(i)

productsGrpByCategory = productsMap.groupByKey()
for i in productsGrpByCategory.take(10): print(i)

(2, <pyspark.resultiterable.ResultIterable object at 0x2897590>)
(4, <pyspark.resultiterable.ResultIterable object at 0x2897750>)
(6, <pyspark.resultiterable.ResultIterable object at 0x2897790>)
(8, <pyspark.resultiterable.ResultIterable object at 0x28977d0>)
(10, <pyspark.resultiterable.ResultIterable object at 0x2897810>)
(12, <pyspark.resultiterable.ResultIterable object at 0x2897850>)
(14, <pyspark.resultiterable.ResultIterable object at 0x2897890>)
(16, <pyspark.resultiterable.ResultIterable object at 0x28978d0>)
(18, <pyspark.resultiterable.ResultIterable object at 0x2897910>)
(20, <pyspark.resultiterable.ResultIterable object at 0x2897950>)

l = productsGrpByCategory.first()
list(l[1])

one_cat_grp = productsGrpByCategory.first()
one_cat_grp[1]

list(one_cat_grp[1]).count()


# sorted() is a python API to sort the pipelined-RDD/python-iterable
help(sorted)

sorted_l = sorted(l[1], key=lambda k:float(k.split(",")[4]) ,reverse=True)

sorted_one_cat_grp = sorted(one_cat_grp[1], key=lambda k:float(k.split(",")[4]) ,reverse=True)
 for i in sorted_one_cat_grp.take(10): print(i)
type(sorted_l)

sorted_l[:3] 
 
# pass sorted() into a flatmap to apply the sorted on the complete RDD 

topNProductsByCategory = productsGrpByCategory.flatMap(lambda p:sorted(p[1], key=lambda k:float(k.split(",")[4]) ,reverse=True)[:3])

for i in topNProductsByCategory.take(10): print(i)










-------------------------------------------------------------------------------------------------------------------------------------------------------
# mysql

CREATE TABLE nn_emp (
  empno    INTEGER ,
  ename    VARCHAR(10),
  job      VARCHAR(9),
  mgr      INTEGER,
  hiredate DATE,
  sal      INTEGER,
  comm     NUMERIC(7,2),
  deptno   NUMERIC(2)
);

STR_TO_DATE('17-09-2010','%d-%m-%Y')

INSERT INTO nn_emp VALUES (7369,'SMITH','CLERK',7902,STR_TO_DATE('17-12-1980','%d-%m-%Y'),800,NULL,20);
INSERT INTO nn_emp VALUES (7499,'ALLEN','SALESMAN',7698,STR_TO_DATE('20-2-1981','%d-%m-%Y'),1600,300,30);
INSERT INTO nn_emp VALUES (7521,'WARD','SALESMAN',7698,STR_TO_DATE('22-2-1981','%d-%m-%Y'),1250,500,30);
INSERT INTO nn_emp VALUES (7566,'JONES','MANAGER',7839,STR_TO_DATE('2-4-1981','%d-%m-%Y'),2975,NULL,20);
INSERT INTO nn_emp VALUES (7654,'MARTIN','SALESMAN',7698,STR_TO_DATE('28-9-1981','%d-%m-%Y'),1250,1400,30);
INSERT INTO nn_emp VALUES (7698,'BLAKE','MANAGER',7839,STR_TO_DATE('1-5-1981','%d-%m-%Y'),2850,NULL,30);
INSERT INTO nn_emp VALUES (7782,'CLARK','MANAGER',7839,STR_TO_DATE('9-6-1981','%d-%m-%Y'),2450,NULL,10);
INSERT INTO nn_emp VALUES (7788,'SCOTT','ANALYST',7566,STR_TO_DATE('13-JUL-87','dd-mm-rr')-85,3000,NULL,20);
INSERT INTO nn_emp VALUES (7839,'KING','PRESIDENT',NULL,STR_TO_DATE('17-11-1981','%d-%m-%Y'),5000,NULL,10);
INSERT INTO nn_emp VALUES (7844,'TURNER','SALESMAN',7698,STR_TO_DATE('8-9-1981','%d-%m-%Y'),1500,0,30);
INSERT INTO nn_emp VALUES (7876,'ADAMS','CLERK',7788,STR_TO_DATE('13-JUL-87', 'dd-mm-rr')-51,1100,NULL,20);
INSERT INTO nn_emp VALUES (7900,'JAMES','CLERK',7698,STR_TO_DATE('3-12-1981','%d-%m-%Y'),950,NULL,30);
INSERT INTO nn_emp VALUES (7902,'FORD','ANALYST',7566,STR_TO_DATE('3-12-1981','%d-%m-%Y'),3000,NULL,20);
INSERT INTO nn_emp VALUES (7934,'MILLER','CLERK',7782,STR_TO_DATE('23-1-1982','%d-%m-%Y'),1300,NULL,10);
COMMIT;

select * from nn_emp

select empno,deptno,sal,AVG(sal) over (PARTITION by deptno) as avg_dept_sal from nn_emp;

SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY deptno) AS avg_dept_sal FROM nn_emp;

SELECT employee, sale, date, SUM(sale) OVER (PARTITION by employee ORDER BY date) AS cum_sales FROM sales;


select empno,deptno,sal,rank() over (partition by deptno order by sal) as rank_dept_sal
from nn_emp

select * from (
select empno,deptno,sal,rank() over (partition by deptno order by sal) as rank_dept_sal
from nn_emp)
where rank_dept_sal <=2
order by 2,4


select empno,deptno,sal,dense_rank() over (partition by deptno order by sal) as rank_dept_sal
from nn_emp

select * from (
select empno,deptno,sal,dense_rank() over (partition by deptno order by sal) as rank_dept_sal
from nn_emp)
where rank_dept_sal <=2
order by 2,4


'''
sqooping data from database into HDFS
'''

sqoop list-databases \
  --connect jdbc:mysql://nn01.itversity.com:3306 \
  --username retail_dba \
  --password itversity
  
sqoop list-tables \
--connect jdbc:mysql://nn01.itversity.com:3306/retail_export \
--username retail_dba \
--password itversity
  
sqoop eval \
--connect jdbc:mysql://nn01.itversity.com:3306/retail_export \
--username retail_dba \
--password itversity \
--query "select * from nn_emp" 


sqoop import \
--connect jdbc:mysql://nn01.itversity.com:3306/retail_export \
--username retail_dba \
--password itversity \
--num-mappers 1 \
--table nn_emp \
--warehouse-dir /user/nandanasgn/sqoop_import/hr_db \
--hive-import \
--hive-database zzzzz_sqoop_import_all_nn\
--hive-table nn_emp

hadoop fs -ls /user/nandanasgn/sqoop_import/hr_db/nn_emp

hadoop fs -cat /user/nandanasgn/sqoop_import/hr_db/nn_emp/part-m-00000


'''
now the desired data is imported into hdfs in text file format
write pyspark QL to access the data
also use core-SPARK-APIs to perform transformations and actions on the data 
'''
  
employees = sc.textFile("/user/nandanasgn/sqoop_import/hr_db/nn_emp/part-m-00000")

emp_map = employees.map(lambda e : (int(e.split(",")[7]),e))

for i in emp_map.take(10): print(i)

emp_group_by_dept = emp_map.groupByKey()

emp_group_by_dept.count()

sorted_emp_10 = sorted(dept_10_emp[1], key=lambda k:float(k.split(",")[5]),reverse=True)

list(sorted_emp_10)
[u'7839,KING,PRESIDENT,null,1981-11-17,5000,null,10', u'7782,CLARK,MANAGER,7839,1981-06-09,2450,null,10', u'7934,MILLER,CLERK,7782,1982-01-23,1300,null,10']

# pass sorted() into a flatmap to apply the sorted on the complete RDD 

topNSalByDept = emp_group_by_dept.flatMap(lambda emp:sorted(emp[1], key=lambda k:float(k.split(",")[5]) ,reverse=True)[:3])

for i in topNSalByDept.take(10): print(i)

7839,KING,PRESIDENT,null,1981-11-17,5000,null,10
7782,CLARK,MANAGER,7839,1981-06-09,2450,null,10
7934,MILLER,CLERK,7782,1982-01-23,1300,null,10
7902,FORD,ANALYST,7566,1981-12-03,3000,null,20
7566,JONES,MANAGER,7839,1981-04-02,2975,null,20
7369,SMITH,CLERK,7902,1980-12-17,800,null,20
7698,BLAKE,MANAGER,7839,1981-05-01,2850,null,30
7499,ALLEN,SALESMAN,7698,1981-02-20,1600,300.00,30
7844,TURNER,SALESMAN,7698,1981-09-08,1500,0.00,30


''' sql counterpart '''

SELECT *
FROM (SELECT my_tab.*,
             RANK () OVER (PARTITION BY deptno ORDER BY sal DESC) AS top_N_sal_by_dept
      FROM nn_emp my_tab)
WHERE top_N_sal_by_dept <= 3
ORDER BY deptno

7839	KING	PRESIDENT			17/11/1981	5000		10	1
7782	CLARK	MANAGER		7839	09/06/1981	2450		10	2
7934	MILLER	CLERK		7782	23/01/1982	1300		10	3
7788	SCOTT	ANALYST		7566	19/04/1987	3000		20	1
7902	FORD	ANALYST		7566	03/12/1981	3000		20	1
7566	JONES	MANAGER		7839	02/04/1981	2975		20	3
7698	BLAKE	MANAGER		7839	01/05/1981	2850		30	1
7499	ALLEN	SALESMAN	7698	20/02/1981	1600	300	30	2
7844	TURNER	SALESMAN	7698	08/09/1981	1500	0	30	3

'''save the data into a file '''
topNSalByDept.saveAsTextFile("/user/nandanasgn/rdd_export/topNSalByDept.txt")

''' sqoop export the result into a parquet file '''

--------------------------------------------------------------------------------------------------------------------------------------

# simple function definition

def min_max(numbers):
''' gives out the min and max(in this order) for a list of numbers'''
	s = l = numbers[0]
	for i in numbers:
		if l<i:
			l=i
		elif s>i:
			s=i
	return s,l
		
		
def min_max(numbers):
  s = l = numbers[0]
  for i in numbers:
    if l<i:
      l=i
    elif s>i:
      s=i
  return s,l	

smallest,largest = min_max([1,2,5,4,76,89,34,56,34543,5656,34343,767567,23234,7675,23432])  
 
print(smallest)
  
print(largest)

# variable length arguments
''' *numbers is treated as a variable length list'''
def varLenArgTotal(initial = 5, *numbers):
  count=initial
  for num in numbers:
    count+=num
  return count
  
tot = varLenArgTotal(25,76,457,23,76,23234,987)  #25 will be treated as initial

print(tot)

tot = varLenArgTotal()
 
print(tot)

# keyword argumetns
# ** arguments will contain key=value hashmap 
def varLenArgTotal(initial = 5, *numbers, **keywords):
  count=initial
  for num in numbers:
    count += num
  for key in keywords:
    count += keywords[key]
  return count	
 
result =  varLenArgTotal(10,1,2,3,veg=50,frt=100)
print(result)

# imp tip !!! - if we have multiple types of arguments in the function signature, keyword arguments must be placed at the end, just prior to that should be variable length arguments, and the at the begining    
  

#LAMBDA FUNCTIONS

  


