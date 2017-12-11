A. create database
------------------
create database retail;

--this creates a folder by the name of retail.db under /user/hive/warehouse

A1. show all the databases in hive
----------------------------------
show databases;

B. Select a database
--------------------
use retail;

B1. Show tables under the database
----------------------------------
show tables;


C1. Create transaction table
-------------------------------
create table txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited
fields terminated by ','
stored as textfile;

C2. Create customer table
-------------------------------
create table customer(custno INT, firstname STRING, lastname STRING, age INT, profession STRING)
row format delimited
fields terminated by ','
stored as textfile;

 
D1. Load the data into the table (from local file system)
-----------------------------------------------------
LOAD DATA LOCAL INPATH '/home/hduser/txns1.txt' OVERWRITE INTO TABLE txnrecords;

LOAD DATA LOCAL INPATH '/home/hduser/custs' OVERWRITE INTO TABLE customer;

LOAD DATA LOCAL INPATH '/home/hduser/custs_add' INTO TABLE customer;


D2. Load the data into the table (from hdfs system)
-----------------------------------------------------
LOAD DATA INPATH '/project/txns1.txt' OVERWRITE INTO TABLE txnrecords;
LOAD DATA INPATH '/crif/custs' OVERWRITE INTO TABLE customer;

trunctate table customer;
hadoop fs -put custs /retail
LOAD DATA INPATH '/retail/custs' OVERWRITE INTO TABLE customer;


D3. Load the data without header
--------------------------------
create table employee_wo_header(empno INT, empname STRING, salary bigint)
row format delimited
fields terminated by ','
stored as textfile
tblproperties("skip.header.line.count"="1");

load data local inpath '/home/hduser/hivedata_header' overwrite into table employee_wo_header;

 
E 1. Describing metadata or schema of the table
---------------------------------------------
describe txnrecords;

E 2. Describing detailed metadata or schema of the table
---------------------------------------------
describe extended txnrecords;

or

describe formatted txnrecords;


F. Counting no of records
-------------------------
select count(*) from txnrecords;

G1. Count of each profession in the Customers List
---------------------------------------------------
select profession, count(*) as headcount from customer group by profession order by headcount;

G2. Top 10 Customers List
------------------------
select a.custno, b.firstname,b.lastname, b.age, b.profession, round(sum(a.amount),2) as amt from txnrecords a, customer b where a.custno=b.custno group by a.custno, b.firstname, b.lastname, b.age, b.profession order by amt desc limit 10;

Dynamic partitioning
--------------------

H1. Create partitioned table (single bucket)
---------------------------------------------
create table txnrecsByCat(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING, spendby STRING)
partitioned by (category STRING)
row format delimited
fields terminated by ','
stored as textfile;


H2. Create partitioned table (with multiple buckets)
--------------------------------------------------
create table txnrecsByCat2(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING, spendby STRING)
partitioned by (category STRING)
clustered by (state) into 10 buckets
row format delimited
fields terminated by ','
stored as textfile;

H3. Create partitioned table (single bucket) on a derived column
----------------------------------------------------------------
create table txnrecsByCat3(txnno INT, txndate STRING, custno INT, amount DOUBLE,
category string, product STRING, city STRING, state STRING, spendby STRING)
partitioned by (month string)
row format delimited
fields terminated by ','
stored as textfile;

H4. Create partitioned table (single bucket) on multiple columns
----------------------------------------------------------------
create table txnrecsByCat4(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING)
partitioned by (category STRING, spendby string)
row format delimited
fields terminated by ','
stored as textfile;


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

set hive.enforce.bucketing=true;


I1. Load data into partition table (single bucket)
---------------------------------------------------
from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat PARTITION(category) select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby, txn.category DISTRIBUTE By category;

I2. Load data into partition table (with multiple buckets)
---------------------------------------------------
from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat2 PARTITION(category) select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby, txn.category DISTRIBUTE By category;

I3. Load data into partition table (single bucket) using a derived partition column
------------------------------------------------------------------------------------
from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat3 PARTITION(month) select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby, txn.category, substring(txn.txndate,1,2) as month DISTRIBUTE By substring(txndate,1,2);

I4. Load data into partition table (single bucket) using multiple partition columns
------------------------------------------------------------------------------------
from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat4 PARTITION(category,spendby) select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.category, txn.spendby DISTRIBUTE By category, spendby;

static partitioning
-------------------
create table txnrecsByCat5(txnno INT, txndate STRING, custno INT, amount DOUBLE,
product STRING, city STRING, state STRING, spendby STRING)
partitioned by (category STRING)
row format delimited
fields terminated by ','
stored as textfile;

from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat5 PARTITION(category='Gymnastics') select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby where txn.category='Gymnastics';

from txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat5 PARTITION(category='Team Sports') select txn.txnno, txn.txndate,txn.custno, txn.amount,txn.product,txn.city,txn.state, txn.spendby where txn.category='Team Sports';




J.create external tables
----------------------
***first load the data set on hadoop

$ hadoop fs -mkdir /user/training

$ hadoop fs -put /home/hduser/custs /user/training

create external table customer(custno string, firstname string, lastname string, age int,profession string)
row format delimited
fields terminated by ','
stored as textfile
location '/user/training';

select * from customer;

describe extended customer;
or
describe formatted customer;


K 1. Storing output in local file
------------------------------
INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/retail/hive/custcount' row format delimited fields terminated by ',' 
select profession, count(*) as headcount from customer group by profession order by headcount desc limit 10;


INSERT OVERWRITE LOCAL DIRECTORY '/home/hduser/mindtree/topten' row format delimited fields terminated by ',' 
select a.custno, b.firstname,b.lastname, b.age, b.profession, round(sum(a.amount),2) as amt from txnrecords a, customer b where a.custno=b.custno group by a.custno, b.firstname, b.lastname, b.age, b.profession order by amt desc limit 10;


K 2. Storing output in hdfs file system
-------------------------------------------
INSERT OVERWRITE DIRECTORY '/retail/custcount' row format delimited fields terminated by ',' 
select profession, count(*) from customer group by profession;


INSERT OVERWRITE DIRECTORY '/retail/topten' row format delimited fields terminated by ',' 
select a.custno, b.firstname,b.lastname, b.age, b.profession, sum(a.amount) as amt from txnrecords a, customer b where a.custno=b.custno group by a.custno, b.firstname, b.lastname, b.age, b.profession order by amt desc limit 10;

INSERT OVERWRITE DIRECTORY '/hanoi/topten' row format delimited fields terminated by ',' 
select a.custno, b.firstname,b.lastname, b.age, b.profession, sum(a.amount) as amt from txnrecords a, customer b where a.custno=b.custno group by a.custno, b.firstname, b.lastname, b.age, b.profession order by amt desc limit 10;


L. to execute script from command prompt
--------------------------------------
$ hive -f filename.sql
$ hive -f professioncount.sql

M. to execute command from command prompt
--------------------------------------
$ hive -e "select * from retail.customer"


N1. how do i know i am in which database currently
--------------------------------------------------
set hive.cli.print.current.db=true;

N2. how do i print my headers of my table
-------------------------------------
set hive.cli.print.header=true;

N3. how do i set my default file format
---------------------------------------
set hive.default.fileformat=textfile;

O.run hive query from linux terminal and copy the result to your own file
-----------------------------------------------------------------------
$ hive -e "select * from retail.customer" > /home/hduser/retail_output.txt

P 1.Create a view in hive for customers whose age is more than 45 years
-----------------------------------------------------------------------
CREATE VIEW age_45plus AS
SELECT * FROM customer
WHERE age>45;

select * from age_45plus;

5354 records

--create a file custs1 on local file system
10000,Mike,Smith,50,Pilot

---place the above file under customer folder on hdfs

hadoop fs -put custs1 /user/hive/warehouse/retail.db/customer

select * from age_45plus;

5355 records 

P 2.Create a view in hive for top 10 customers 
----------------------------------------------
CREATE VIEW topten AS
select a.custno, b.firstname,b.lastname, b.age, b.profession, round(sum(a.amount),2) as amt from txnrecords a, customer b where a.custno=b.custno group by a.custno, b.firstname, b.lastname, b.age, b.profession order by amt desc limit 10;

select * from topten;


Q. inserting output into another table ( make sure Airsports table is created beforehand)
---------------------------------------------------------------------------------------
create table Airsports(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited
fields terminated by ','
stored as textfile;

insert overwrite table Airsports select * from txnrecords where category = 'Air Sports';

select * from Airsports;


R1. find sales done in each payment mode and their percentage
--------------------------------------------------------------
create table totalsales (total bigint)
row format delimited                                                                                  
fields terminated by ',';   

insert overwrite table totalsales                                                                           
select sum(amount) from txnrecords;

select a.spendby, round(sum(a.amount),2) as typesales, round((sum(a.amount)/total*100),2) as salespercent from txnrecords a, totalsales b group by a.spendby, b.total ;


R2.find sales based on age group with the % on totalsales
---------------------------------------------------------

create table out1 (custno int,firstname string,age int,profession string,amount double,product string)
row format delimited                                                                                  
fields terminated by ',';   


insert overwrite table out1                                                                           
select a.custno,a.firstname,a.age,a.profession,b.amount,b.product                                     
from customer a JOIN txnrecords b ON a.custno = b.custno;     

select * from out1 limit 100;

create table out2 (custno int,firstname string,age int,profession string,amount double,product string, level string)
row format delimited                                                                                  
fields terminated by ',';   

insert overwrite table out2
select * , case when age<30 then 'low' when age>=30 and age < 50 then 'middle' when age>=50 then 'old' 
else 'others' end
from out1;


select * from out2 limit 100; 

describe out2;  

create table out3 (level string, amount double, salespercent double)                      
row format delimited
fields terminated by ',';

insert overwrite table out3  
select a.level, round(sum(a.amount),2) as totalspent, round((sum(a.amount)/total*100),2) as salespercent  from out2 a, totalsales b group by a.level, b.total;


select * from out3;



S.create an index on customer (earlier created) table on profession column
--------------------------------------------------------------------------
use retail;

*** deferred rebuild will create an empty index
create index prof_index on table customer(profession) as 'compact' with deferred rebuild;

**** alter index will actually create the index
alter index prof_index on customer rebuild;

******list all the indexes on the table
show indexes on customer;

*****schema of the index
describe retail__customer_prof_index__;

select * from retail__customer_prof_index__ where profession = "Pilot;

****Time taken without index
-----------------------------
select profession, count(*) from customer group by profession;

list of all the profession and the count of customers

Time taken: 45.003 seconds, Fetched: 51 row(s)


****Time taken with index
--------------------------
select profession, SIZE(`_offsets`) from niit__customer_prof_index__;

list of all the profession and the count of customers

Time taken: 0.0604 seconds, Fetched: 51 row(s)




T. Joins in hive
----------------
****emp.txt
****swetha,250000,Chennai
****anamika,200000,Kanyakumari
****tarun,300000,Pondi
****anita,250000,Selam


****email.txt
****swetha,swetha@gmail.com
****tarun,tarun@edureka.in
****nagesh,nagesh@yahoo.com
****venkatesh,venki@gmail.com


create table employee(name string, salary float,city string)
row format delimited
fields terminated by ',';

load data local inpath '/home/hduser/emp.txt' into table employee;

select * from employee;

create table mailid (name string, email string)
row format delimited
fields terminated by ',';


load data local inpath '/home/hduser/email.txt' into table mailid;

select * from mailid;

inner join
----------
select a.name,a.city,a.salary,b.email from 
employee a join mailid b on a.name = b.name;

outer joins
-----------
select a.name,a.city,a.salary,b.email from 
employee a left outer join mailid b on a.name = b.name;


select b.name,a.city,a.salary,b.email from 
employee a right outer join mailid b on a.name = b.name;

select a.name,a.city,a.salary,b.email from 
employee a full outer join mailid b on a.name = b.name;


U. Setting up local variables and parameters in hive
-----------------------------------------------------
hive> set myage=25;
hive> select * from customer where age >= ${hiveconf:myage};

similarly, you could pass on command line:

$ hive -hiveconf myage=25 -f professioncount.sql

nano professioncount.sql
------------------------
select profession, count(profession) from retail.customer where age >= ${hiveconf:myage} group by profession order by profession;


To see all the available variables, from the command line, run

$ hive -e 'set;'

or from the hive prompt, run

hive> set;

one can use hivevar variables as well, putting them into sql snippets or can be included from hive CLI using the source command (or pass as -i option from command line). The benefit here is that the variable can then be used with or without the hivevar prefix, and allow something akin to global vs local use.

So, assume have some setup.sql which sets a tablename variable:

hive> set hivevar:tablename=customer;

then, I can bring into hive:

hive> source /home/hduser/customer.sql;

customer.sql
------------
hive> select * from ${tablename};

or

hive> select * from ${hivevar:tablename};


--Could also set a "local" tablename, which would affect the use of ${tablename}, but not ${hivevar:tablename}

--hive> set tablename1=txnrecords;

set hivevar:tablename=txnrecords;

hive> select * from ${tablename1};

vs

hive> select * from ${hivevar:tablename};



Control of number of mappers in hive
------------------------------------
set mapreduce.input.fileinputformat.split.minsize=134217728;

if you want to combine multiple small files
-------------------------------------------
set mapreduce.input.fileinputformat.split.maxsize=134217728;


V. User Define Functions
-------------------------
create table testing(id string,unixtime string)
row format delimited
fields terminated by ',';

load data local inpath '/home/hduser/counter.txt' into table testing;

hive> select * from testing;

****OK
****one		1386023259550
****two		1389523259550
****three	1389523259550
****four	1389523259550

******* adding the jar in the hive script *********
add jar /home/hduser/udfhive.jar;

****** to display the jar files in hive *********
list jars;

******define user function ************
create temporary function userdate as 'udfhive.UnixtimeToDate';


****Then use function 'userdate' in sql command

select id, userdate(unixtime) from testing;


W. Loading Avro type-data (flume) in Hive table
--------------------------------------------
CREATE TABLE tweets
  ROW FORMAT SERDE
     'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ('avro.schema.url'='file:/home/hduser/intel.avsc') ;

LOAD DATA Local INPATH '/home/hduser/FlumeData.1504749217753' OVERWRITE INTO TABLE tweets;



CREATE TABLE tweets2 (
id string, 
user_friends_count int, 
user_location string,
user_description string,
user_statuses_count int,
user_followers_count int,
user_name string,
user_screen_name string,
created_at string,
text string,
retweet_count bigint,
retweeted boolean,
in_reply_to_user_id bigint,
source string,
in_reply_to_status_id bigint,
media_url_https string,
expanded_url string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t' 
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE tweets2 SELECT * FROM tweets;


X. Convert Text file to Avro Format
-----------------------------------
create database college;
use college;

students.csv
------------
Amit,Maths,91
Amit,Physics,48
Amit,Chemistry,66
Sanjay,Maths,96
Sanjay,Physics,64
Sanjay,Chemistry,73

Create a Hive table stored as textfile

CREATE TABLE csv_table (
student_name string,
subject string,
marks INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

--2. Load csv_table with student.csv data
LOAD DATA LOCAL INPATH "/home/hduser/students.csv" OVERWRITE INTO TABLE csv_table;

--3. Create another Hive table using AvroSerDe
CREATE TABLE avro_table
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
    'avro.schema.literal'='{
      "namespace": "abc",
      "name": "student_marks",
      "type": "record",
      "fields": [ { "name":"student_name","type":"string"}, { "name":"subject","type":"string"}, { "name":"marks","type":"int"}]
    }');

--4. Load avro_table with data from csv_tabl
INSERT OVERWRITE TABLE avro_table SELECT student_name, subject, marks FROM csv_table;

--Now you can get data in Avro format from Hive warehouse folder. To dump this file to local file system use below command:

 hadoop fs -cat /user/hive/warehouse/college.db/avro_table/* > student.avro

---5 Create and Load data in ORC format

CREATE TABLE orc_table (
student_name string,
subject string,
marks INT)
STORED AS ORC;

INSERT OVERWRITE TABLE orc_table SELECT student_name, subject, marks FROM csv_table;


6. Create a sequence File format and load data from another table
------------------------------------------------------------------
CREATE TABLE seq_table (
student_name string,
subject string,
marks INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE seq_table SELECT student_name, subject, marks FROM csv_table;




--If you want to get json data from this avro file you can use avro tools command:
-- jar file is not available
--java -jar avro-tools-1.7.5.jar tojson student.avro > student.json



analysis of data from mapreduce output
-------------------------------------
create external table margin(prodno string, qty int, profit bigint, margin_pc double)
row format delimited
fields terminated by ','
stored as textfile
location '/retail/margin_data';


Y.hive allows to read  data from sub dir
----------------------------------------
set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;

Z. hive allows to run select statement without mapreduce
---------------------------------------------------------
set hive.fetch.task.conversion=more;




