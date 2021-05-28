###파티션 테이블

#외부테이블 만들기 
#이때 로케이션의 포트번호는 hdfs-site.xml의 rpc포트정보이다.
create external table airline_delay_raw(
year int,
month int,
dayofmonth int,
dayofweek int,
deptime int,
crsdeptime int,
arrtime int,
crsarrtime int,
uniquecarrier string,
flightnum int,
tailnum string,
actualelapsedtime int,
crselapsedtime int,
airtime int,
arrdelay int,
depdelay int,
origin string,
dest string, 
distance int,
taxiin int,
taxiout int,
cancelled int,
cancellationcode string,
diverted int,
carrierdelay string,
weatherdelay string,
nasdelay string,
secutirydelay string,
lateaircraftdelay string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location 'hdfs://hadoop01:8020/user/hadoop01/input';

#데이터 insert
insert overwrite table airline_delay partition (delayyear = '2005')
select * 
from airline_delay_raw
where year = 2005;

#해당 테이블의 전체 파티션정보 확인하기
show partitions airline_delay;

#다이나믹 파티션 사용하기
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=100;

insert overwrite table airline_delay partition (delayyear)
select *
from airline_delay_raw
where year = 2005;

#다시 파티션 정보 조회하기
show partitions airline_delay;

