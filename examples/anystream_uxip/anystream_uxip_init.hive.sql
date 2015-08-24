
CREATE DATABASE IF NOT EXISTS anystream_uxip;

USE anystream_uxip;

CREATE TABLE IF NOT EXISTS bdl_fdt_uxip_events (
  app_name       STRING  COMMENT 'application name'
, app_ver        STRING  COMMENT 'application version'
, session_id     STRING  COMMENT 'session identity'
, session_enter  BIGINT  COMMENT 'start timestamp of session'
, session_exit   BIGINT  COMMENT 'exit  timestamp of session'
, platform       MAP<STRING, STRING>         COMMENT 'platform info'
, misc           ARRAY<MAP<STRING, STRING>>  COMMENT 'event info'
, ext_domain     MAP<STRING, STRING>         COMMENT 'extensive domain'
) PARTITIONED BY (stat_date BIGINT COMMENT 'partition date');
