#====이너조인
select a.year, a.origin, b.airport, a.dest, c.airport, count(*)
from airline_delay a 
join airport_code b on ( a.origin = b.iata )
join airport_code c on ( a.dest = c.iata )
where a.arrdelay > 0
group by a.year, a.origin, b.airport, a.dest, c.airport;

#====데이터의 불필요문자("")를 지우기 위해 
#임시 테이블인 tmp를 만들고 옮긴다.

create table carrier_code2(
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

load data local inpath '/home/hadoop01/meta/carriers_new.csv'
overwrite into table tmp;

insert overwrite table carrier_code2
select regexp_replace(code, '\"', ''), description
from tmp;

drop table tmp;

select * from carrier_code2 limit 20;

#====아우터조인
select a.year, a.uniquecarrier, b.code, b.description
from airline_delay a left outer join carrier_code2 b
on (a.uniquecarrier = b.code)
where a.uniquecarrier = 'WN'
limit 10;