
USE anystream;

abc_d_ := 
insert overwrite table xxx 
SELECT  x.c1 AS c1
	  , y.c2 AS c2
FROM xxx x JOIN yyy y
ON x.id = y.id;

raw_data :=
SELECT  partition
      , send_timestamp
      , split(utf8(one_row), '\\u0011') as data_row
      , ext_domain
FROM  `__llbase__`;

insert into table cytest_uxip
USING 'jdbc:mysql://172.16.10.66:3306/db_orion?user=mz_orion&password=mz_orion_pwd'
SELECT  data_row[0]
      , partitioner(partition, send_timestamp)
FROM raw_data;

insert into table dbo.cytest_uxip 
USING 'jdbc:sqlserver://172.16.200.77:1433\;DatabaseName=as_material\;user=test_dl\;password=123456'
SELECT  data_row[0]
      , partitioner(partition, send_timestamp)
FROM raw_data; 


insert into table cytest_uxip
USING 'jdbc:oracle:thin:dwarch/etl_meizu123@172.16.10.175:1521:DWARCH'
SELECT  data_row[0]
      , partitioner(partition, send_timestamp)
FROM raw_data; 

insert into table cytest_uxip
USING 'jdbc:phoenix:172.16.200.239,172.16.200.233,172.16.200.234:218'
SELECT  data_row[0]
      , partitioner(partition, send_timestamp)
FROM raw_data; 


--SELECT  
--	cast(data AS STRING),  
--	partitioner(partition, send_timestamp) 
--FROM `__llbase__`; 

--insert into directory 'metaq://zkCluster/topicname'
--SELECT ext_domain, data_row, partitioner(partition, send_timestamp) FROM raw_data; 
