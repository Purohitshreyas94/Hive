
create database analytics;

use analytics;

Create table without header:-
----------------------------

create table airlines
(
year int,
quarter int,
avg_revenue double,
booked_seats bigint
)
row format delimited
fields terminated by ','
stored as textfile
TBLPROPERTIES("skip.header.line.count"="1");


Load data in table from local system:-
-------------------------------------
LOAD DATA LOCAL INPATH '/home/hduser/airlines.csv' OVERWRITE INTO TABLE airlines;


select * from airlines; 

select count(*) from airlines; 

select distinct(year) from airlines; 


======================================================================================================================================================


I)Find the total sales/revenue done in each year.

-> select year,cast(sum(avg_revenue*booked_seats) as bigint) as totalsales from airlines group by year;


------------------------------------------------------------------------------------------------------------------------------------------------------

II)Find the total sales in each year in Million.

-> select year,round(sum(avg_revenue*booked_seats)/1000000, 2) as totalsales from airlines group by year;


-------------------------------------------------------------------------------------------------------------------------------------------------------

III)Find Growth of sales on each year
->
growth = (next year sales -first year sales)/first year sales *100

year 	sales
1995	43.49
1996	46.36

like growth for 1996 is

growth of 1996 = (46.36-43.49)/43.49*100
     	  1996 = 6.59

like this for all 21 years

-------------------------------------------------------------------------------------------------------------------------------------------------------


IV)Find Number of passengers travelled in each year.

-> select year,sum(booked_seats) as totalseats from airlines group by year;

-------------------------------------------------------------------------------------------------------------------------------------------------------


V)Find Growth of Passenger in each year
->
growth = (next year passenger -first year passenger)/first year passenger *100

year 	sales
1995	148520
1996	167223

like growth for 1996 is

growth of 1996 = (167223-148520)/148520*100
     	  1996 = 12.59

like this for all 21 years


------------------------------------------------------------------------------------------------------------------------------------------------------


VI)Find Maximum revenue/sales on each quarter.
->
select quarter,cast(max(avg_revenue*booked_seats) as bigint) as maxsales from airlines group by quarter;




=======================================================================================================================================================




