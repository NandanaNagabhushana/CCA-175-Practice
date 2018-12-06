'''
MySQL practice
----------------

Hostname: ms.itversity.com
Several Databases for different data sets :
	retail_db
	hr_db
	nyse_db
Users :
	retail_user
	hr_user
	nyse_user
Password: itversity

'''

# login

mysql -u retail_user -h ms.itversity.com -p

show databases;

use retail_db;

show tables;

select count(1) from order_items;


