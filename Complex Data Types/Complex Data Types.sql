
create database complex;


use complex;

------------------------------------------------------------------------------------------------------------------------------------------------------

1)ARRAY:-
===========

$ cat >arrayfile

1,abc,40000,a$b$c,hyd
2,def,3000,d$f,bang



create table tab7(id int,
name string,
sal bigint,
sub array<string>,
city string) 
row format delimited fields terminated by ',' 
collection items terminated by '$';


load data local inpath '/home/hduser/arrayfile' overwrite into table tab7;


select * from tab7;

select * from tab7 where id=1;

select id, name, sal, sub[2] from tab7;
  

-------------------------------------------------------------------------------------------------------------------------------------------------------


2)MAP:-
==========

$ cat >mapfile

1,abc,40000,a$b$c,pf#500$epf#200,hyd
2,def,3000,d$f,pf#500,bang



create table tab10(id int,
name string,
sal bigint,
sub array<string>,
dud map<string,int>,
city string)
row format delimited 
fields terminated by ','
collection items terminated by '$'
map keys terminated by '#';

load data local inpath '/home/hduser/mapfile' overwrite into table tab10;

hive>select * from tab10;

hive>select * from tab10 where dud["bonus"]>0;

hive>select * from tab10 where dud["insurance"] is not null;

hive>select sum(dud["bonus"]) from tab10 where dud["bonus"] is not null;

hive>select dud["bonus"],dud["insurance"] from tab10;
 
hive>select dud["pf"] from tab10; 

hive>select dud["pf"],dud["epf"] from tab10; 


-------------------------------------------------------------------------------------------------------------------------------------------------------


3)STRUCT:-
===========

cat >structfile

1,abc,40000,a$b$c,pf#500$epf#200,hyd$ap$500001
2,def,3000,d$f,pf#500,bang$kar$600038



create table tab11(id int,
name string,
sal bigint,
sub array<string>,
dud map<string,int>,
addr struct<city:string,
state:string,pin:bigint>)
row format delimited 
fields terminated by ','
collection items terminated by '$'
map keys terminated by '#';



hive>select addr.city,addr.state,addr.pin from tab11;


hive>select * from tab11 where addr.pin=222;


hive>select * from tab11;


=======================================================================================================================================================


