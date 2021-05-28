#====버킷사용

#버킷은 버킷 컬럼 해시를 기준으로 데이터를 지정된 개수의 파일로 분리해서 저장
create table airline_delay2(
year int,
month int,
uniquecarrier string,
arrdelay int,
depdelay int
)
clustered by (uniquecarrier) into 20 buckets;


#2007년 항공운항 지연 데이터 새로운 테이블에 등록
insert overwrite table airline_delay2
select year, month, uniquecarrier, arrdelay, depdelay
from airline_delay
where delayyear = 2007;

#hdfs에 20개의 파일이 생성된 것을 확인할 수 있다.
#20개중에서 첫 번째 버킷만 select문에 사용하기

select uniquecarrier, count(*)
from airline_delay2
tablesample(bucket 1 out of 20)
group by uniquecarrier;
