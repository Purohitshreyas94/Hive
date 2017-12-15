Retail Store data D01,D02,D11,D12:-
---------------------------------


create database retail_store;

use retail_store;

create table retailstore
(date_time string,
custid string,
age string,
residence_area string,
category string,
productid string,
qty int,
cost bigint,
sales bigint)
row format delimited
fields terminated by '\;'
stored as textfile;

--------------------------------------------
uploading data from local system to hdfs:-

LOAD DATA LOCAL INPATH '/home/hduser/D01' OVERWRITE INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/D02'  INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/D11'  INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/D12'  INTO TABLE retailstore;



A. Find the total number of records in table for all month
-> select count(*) from retailstore;


B. Find the total number of records in table for each month
-> select month(date_time),count(*) from retailstore group by month(date_time);


C. To find Records which is null
-> select * from retailstore where sales is null or cost is null; 


D. To check whether fields sales and costs are less or equals to zero 
-> select * from retailstore where sales<= 0 or cost<=0;



create table retailstore1
(date_time string,
custid string,
age string,
residence_area string,
category string,
productid string,
qty int,
cost bigint,
sales bigint)
row format delimited
fields terminated by '\;'
stored as textfile;



E. To insert data to retailstore1 from retailstore
-> insert overwrite table retailstore1 
   select * from retailstore where sales>0 and cost>0;


select count(*) from retailstore1; 


select * from retailstore1 where sales<= 0 or cost<=0;


=======================================================================================================================================================


A1) Find out the customer I.D for the customer and the date of transaction who has spent the maximum amount in a month and in all the 4 months. 
   Answer would be - total 5 customer IDs
1) One for each month
2) One for all the 4 months.

-> I) One for each Month:- 
   ----------------------

INSERT OVERWRITE DIRECTORY '/Hive/retail/HighTransInJan' row format delimited fields terminated by ',' 
 select custid,sales,date_time from retailstore1 a where month(a.date_time) = 1 and sales in (select max(sales) from retailstore1 b where month(b.date_time) = 1);


INSERT OVERWRITE DIRECTORY '/Hive/retail/HighTransInFeb' row format delimited fields terminated by ',' 
 select custid,sales,date_time from retailstore1 a where month(a.date_time) = 2 and sales in (select max(sales) from retailstore1 b where month(b.date_time) = 2);


INSERT OVERWRITE DIRECTORY '/Hive/retail/HighTransInNov' row format delimited fields terminated by ',' 
select custid,sales,date_time from retailstore1 a where month(a.date_time) = 11 and sales in (select max(sales) from retailstore1 b where month(b.date_time) = 11);


INSERT OVERWRITE DIRECTORY '/Hive/retail/HighTransInDec' row format delimited fields terminated by ',' 
select custid,sales,date_time from retailstore1 a where  month(a.date_time) = 12 and sales in (select max(sales) from retailstore1 b where month(b.date_time) = 12);



II) One for all the 4 months:-
------------------------------

INSERT OVERWRITE DIRECTORY '/Hive/retail/HighTransIn4Months' row format delimited fields terminated by ',' 
select custid,sales,date_time from retailstore1 where sales in (select max(sales) from retailstore1);



=======================================================================================================================================================


(A2)Find total gross profit made by each product and also by each category for all the 4 months data.
->
INSERT OVERWRITE DIRECTORY '/Hive/retail/GrossProfitByProduct' row format delimited fields terminated by ',' 
select productid, sum(sales - cost) as profit from retailstore1 group by productid order by profit desc limit 5;


INSERT OVERWRITE DIRECTORY '/Hive/retail/GrossProfitByCategory' row format delimited fields terminated by ',' 
select category, sum(sales - cost) as profit from retailstore1 group by category order by profit desc limit 5;


=======================================================================================================================================================


(A3)Find total gross profit % made by each product and also by each category for all the 4 months data.
-> 
INSERT OVERWRITE DIRECTORY '/Hive/retail/GrossProfitPercentByProduct' row format delimited fields terminated by ','
select productid, round((sum(sales)-sum(cost))/sum(cost)*100,2) as margin from retailstore1 group by productid order by margin desc limit 5; 


INSERT OVERWRITE DIRECTORY '/Hive/retail/GrossProfitPercentByCategory' row format delimited fields terminated by ',' 
select category, round((sum(sales)-sum(cost))/sum(cost)*100,2) as margin from retailstore1 group by category order by margin desc limit 5;


=======================================================================================================================================================



(B)Find out the top 4 or top 10 product being sold in the monthly basis and in all the 4 months.. Criteria for top should be sales amount.
-> 
I)In the Monthly Basis:-
-------------------------

INSERT OVERWRITE DIRECTORY '/Hive/retail/Top10ProductInJan' row format delimited fields terminated by ',' 
select productid, sum(sales) as amount from retailstore1 where month(date_time) = 1 group by productid order by amount desc limit 10;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top10ProductInFeb' row format delimited fields terminated by ',' 
select productid, sum(sales) as amount from retailstore1 where month(date_time) = 2 group by productid order by amount desc limit 10;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top10ProductInNov' row format delimited fields terminated by ',' 
select productid, sum(sales) as amount from retailstore1 where month(date_time) = 11 group by productid order by amount desc limit 10;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top10ProductInDec' row format delimited fields terminated by ',' 
select productid, sum(sales) as amount from retailstore1 where month(date_time) = 12 group by productid order by amount desc limit 10;


II)In All the 4 Months:-
---------------------------

INSERT OVERWRITE DIRECTORY '/Hive/retail/Top10ProductIn4Months' row format delimited fields terminated by ',' 
select productid, sum(sales) as amount from retailstore1 group by productid order by amount desc limit 10;



=======================================================================================================================================================



(C1)Find out the (top 5*) viable products and the (top 5*) product subclass for the age group A, B, C etc..... Data should be taken for all the 4  months
->
I) Viable Products:- 
----------------------

create table table_profa (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profa
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="A"  order by totalsales desc limit 5;

create table table_profb (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profb
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="B"  order by totalsales desc limit 5;

create table table_profc (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profc
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="C"  order by totalsales desc limit 5;

create table table_profd (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profd
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="D"  order by totalsales desc limit 5;

create table table_profe (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profe
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="E"  order by totalsales desc limit 5;

create table table_proff (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_proff
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="F"  order by totalsales desc limit 5;

create table table_profg (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profg
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="G"  order by totalsales desc limit 5;

create table table_profh (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profh
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="H"  order by totalsales desc limit 5;

create table table_profi (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profi
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="I"  order by totalsales desc limit 5;

create table table_profj (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profj
select productid,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, productid having trim(age)="J"  order by totalsales desc limit 5;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top5ViableProductsAgeWise' row format delimited fields terminated by ',' 
select * from(
select productid,age,totalsales from table_profa
UNION
select productid,age,totalsales from table_profb
UNION
select productid,age,totalsales from table_profc
UNION
select productid,age,totalsales from table_profd
UNION
select productid,age,totalsales from table_profe
UNION
select productid,age,totalsales from table_proff
UNION
select productid,age,totalsales from table_profg
UNION
select productid,age,totalsales from table_profh
UNION
select productid,age,totalsales from table_profi
UNION
select productid,age,totalsales from table_profj) top5viableproduct
order by age,totalsales desc; 


I) Viable Categories :- 
-----------------------

create table table_profca (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profca
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="A"  order by totalsales desc limit 5;

create table table_profcb (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcb
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="B"  order by totalsales desc limit 5;

create table table_profcc (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcc
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="C"  order by totalsales desc limit 5;

create table table_profcd (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcd
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="D"  order by totalsales desc limit 5;

create table table_profce (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profce
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="E"  order by totalsales desc limit 5;

create table table_profcf (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcf
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="F"  order by totalsales desc limit 5;

create table table_profcg (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcg
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="G"  order by totalsales desc limit 5;

create table table_profch (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profch
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="H"  order by totalsales desc limit 5;

create table table_profci (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profci
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="I"  order by totalsales desc limit 5;

create table table_profcj (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_profcj
select category,age, (sum(sales)-sum(cost)) as totalsales from retailstore1 group by age, category having trim(age)="J"  order by totalsales desc limit 5;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top5ViableCategoriesAgeWise' row format delimited fields terminated by ',' 
select * from(
select category,age,totalsales from table_profca
UNION
select category,age,totalsales from table_profcb
UNION
select category,age,totalsales from table_profcc
UNION
select category,age,totalsales from table_profcd
UNION
select category,age,totalsales from table_profce
UNION
select category,age,totalsales from table_profcf
UNION
select category,age,totalsales from table_profcg
UNION
select category,age,totalsales from table_profch
UNION
select category,age,totalsales from table_profci
UNION
select category,age,totalsales from table_profcj) top5viablepcategory
order by age,totalsales desc; 



=======================================================================================================================================================


(C2)Find out the (top 5*) loss making products and the (top 5*) loss making product subclass for the age group A, B, C etc..... Data should be taken for all the 4 months
->
I)Top Loss Making Products:-
-----------------------------

create table table_losspa (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspa
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="A"  order by totalsales desc limit 5;

create table table_losspb (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspb
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="B"  order by totalsales desc limit 5;

create table table_losspc (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspc
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="C"  order by totalsales desc limit 5;

create table table_losspd (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspd
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="D"  order by totalsales desc limit 5;

create table table_losspe (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspe
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="E"  order by totalsales desc limit 5;

create table table_losspf (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspf
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="F"  order by totalsales desc limit 5;

create table table_losspg (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspg
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="G"  order by totalsales desc limit 5;

create table table_lossph (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_lossph
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="H"  order by totalsales desc limit 5;

create table table_losspi (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspi
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="I"  order by totalsales desc limit 5;

create table table_losspj (productid string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losspj
select productid,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, productid having trim(age)="J"  order by totalsales desc limit 5;


INSERT OVERWRITE DIRECTORY '/Hive/retail/Top5LossByProductsAgeWise' row format delimited fields terminated by ',' 
select * from(
select productid,age,totalsales from table_losspa
UNION
select productid,age,totalsales from table_losspb
UNION
select productid,age,totalsales from table_losspc
UNION
select productid,age,totalsales from table_losspd
UNION
select productid,age,totalsales from table_losspe
UNION
select productid,age,totalsales from table_losspf
UNION
select productid,age,totalsales from table_losspg
UNION
select productid,age,totalsales from table_lossph
UNION
select productid,age,totalsales from table_losspi
UNION
select productid,age,totalsales from table_losspj) top5lossbyproduct
order by age,totalsales desc; 



II)Top Loss Making Categories:-
---------------------------------
->

create table table_lossca(category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_lossca
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="A"  order by totalsales desc limit 5;

create table table_losscb (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losscb
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="B"  order by totalsales desc limit 5;

create table table_losscc (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losscc
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="C"  order by totalsales desc limit 5;

create table table_losscd (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';
	
insert overwrite table table_losscd
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="D"  order by totalsales desc limit 5;

create table table_lossce (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_lossce
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="E"  order by totalsales desc limit 5;

create table table_losscf (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losscf
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="F"  order by totalsales desc limit 5;

create table table_losscg (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losscg
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="G"  order by totalsales desc limit 5;

create table table_lossch (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_lossch
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="H"  order by totalsales desc limit 5;

create table table_lossci (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_lossci
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="I"  order by totalsales desc limit 5;

create table table_losscj (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_losscj
select category,age, (sum(cost)-sum(sales)) as totalsales from retailstore1 group by age, category having trim(age)="J"  order by totalsales desc limit 5;

INSERT OVERWRITE DIRECTORY '/Hive/retail/Top5LossByCategoriesAgeWise' row format delimited fields terminated by ',' 
select * from(
select category,age,totalsales from table_lossca
UNION
select category,age,totalsales from table_losscb
UNION
select category,age,totalsales from table_losscc
UNION
select category,age,totalsales from table_losscd
UNION
select category,age,totalsales from table_lossce
UNION
select category,age,totalsales from table_losscf
UNION
select category,age,totalsales from table_losscg
UNION
select category,age,totalsales from table_lossch
UNION
select category,age,totalsales from table_lossci
UNION
select category,age,totalsales from table_losscj) top5lossbycategory
order by age,totalsales desc; 




======================================================================================================================================================








