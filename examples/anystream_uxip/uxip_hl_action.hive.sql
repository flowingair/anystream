
USE anystream_uxip;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET anystream.dataframe.partitions=20;

result :=
SELECT  app_name
      , app_ver
      , session_id
      , session_enter
      , session_exit
      , platform_info
      , misc
      , ext_domain
      , partitioner(partition, send_timestamp) 
FROM `__hlbase__`; 

INSERT INTO TABLE bdl_fdt_uxip_events PARTITION (stat_date)
SELECT * FROM result;
