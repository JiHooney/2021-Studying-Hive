#====조회
select year, month, avg(arrdelay) as avg_arrive_delay_time, avg(depdelay) as avg_departure_delay_time 
from airline_delay
where delayyear = 2007
and arrdelay > 0
group by year, month;

#====생성
create table carrier_code2(
code string,
description string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;

create table carrier_code(
code string,
description string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;

create table tmp(
code string,
description string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;

load data local inpath '/home/hadoop01/meta/carriers.csv'
overwrite into table tmp;

load data local inpath '/home/hadoop01/meta/carriers.csv'
overwrite into table carrier_code;

load data local inpath '/home/hadoop01/meta/carriers_new.csv'
overwrite into table carrier_code2;


select a.year, a.uniquecarrier, b.description, count(*)
from airline_delay a join carrier_code2 b
on (a.uniquecarrier = b.code)
where a.arrdelay > 0
group by a.year, a.uniquecarrier, b.description;

insert overwrite table carrier_code
select regexp_replace(code, '\"', ''), description
from tmp;

insert overwrite table carrier_code2
select regexp_replace(code, '\"', ''), description
from carrier_code;

create table temp(
iata string,
airport string,
city string, 
state string, 
country string, 
lat double,
longitude double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;

load data local inpath '/home/hadoop01/meta/airports.csv'
overwrite into table temp;


create table airport_code(
iata string,
airport string,
city string, 
state string, 
country string, 
lat double,
longitude double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;