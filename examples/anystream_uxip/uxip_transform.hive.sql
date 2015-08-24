
USE anystream_uxip;

SET anystream.dataframe.partitions=0;

base :=  SELECT  * FROM `__root__` WHERE interface = 'as_uxip';

data_src := SELECT partition, send_timestamp,  ext_domain, one_row FROM base LATERAL VIEW explode(data) mutliRows AS one_row;

raw_data := 
SELECT  partition
      , send_timestamp
      , split(utf8(one_row), '\\u0011') AS data_row
      , ext_domain 
FROM  data_src; 

uxip_records :=
SELECT  data_row[0] AS app_name
      , data_row[1] AS app_ver
      , data_row[2] AS session_id
      , cast(data_row[3] AS BIGINT) AS session_enter
      , cast(data_row[4] AS BIGINT) AS session_exit
      , IF ( data_row[5] IS NOT NULL, str_to_map(data_row[5],'\\u0012','\\u0013'), map()) AS platform_info
      , str_to_array_of_map(data_row[6],'\\u0012','\\u0013','\\u0014')                    AS misc
      , partition
      , send_timestamp
      , ext_domain
FROM raw_data WHERE data_row IS NOT NULL; 

