
USE anystream_uxip;

SET anystream.dataframe.partitions=16;

behavior :=
SELECT  app_name
      , app_ver
      , session_id
      , cast(session_enter AS STRING) AS session_enter
      , cast(session_exit  AS STRING) AS session_exit
      , coalesce(platform_info['_imei_'], '')         AS userid
      , coalesce(platform_info['_device_'], '')       AS device
      , coalesce(platform_info['_os_version_'], '')   AS os_ver 
      , coalesce(platform_info['_country_'], '')      AS country 
      , coalesce(platform_info['_operator_'], '')     AS operator 
      , coalesce(platform_info['_root_'], '')         AS root 
      , coalesce(platform_info['_flyme_ver_'], '')    AS flyme_ver
      , coalesce(platform_info['_ip_'], '')           AS client_ip
      , cast(unix_timestamp() AS STRING)              AS server_time
      , coalesce(event['_type_'], '')                 AS event_type
      , coalesce(event['_name_'], '')                 AS event_name
      , coalesce(event['_time_'], '')                 AS event_time
      , coalesce(event['_network_'], '')              AS network
      , coalesce(event['_page_'], '')                 AS event_page
      , filter_not(event 
          , ARRAY('_type_', '_name_', '_time_', '_network_', '_page_')
        )                                             AS event_args
FROM `__llbase__` LATERAL VIEW explode(misc) eventsList AS event
WHERE event['_type_'] IN ('action', 'action_x')
AND trim(app_name) IN ('com.meizu.media.video', 'com.meizu.mstore')
AND platform_info['_sn_'] RLIKE '^.{3}A.*';

INSERT INTO DIRECTORY 'metaq://zk-basic-ns1.meizu.mz:2181,zk-basic-ns2.meizu.mz:2181,zk-basic-ns3.meizu.mz:2181/ORION_USER_EVENT_ALI_TOPIC'
SELECT binary(
         concat_ws(''
           , app_name
           , app_ver
           , session_id
           , session_enter
           , session_exit
           , userid
           , device
           , os_ver
           , country
           , operator
           , root
           , flyme_ver
           , client_ip
           , server_time
           , event_type
           , event_name
           , event_time
           , network
           , event_page
           , map_to_str(event_args, '', '')
         )
       )
FROM behavior; 
