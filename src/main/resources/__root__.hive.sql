
CREATE DATABASE IF NOT EXISTS anystream;

CREATE TABLE IF NOT EXISTS root(
interface      String  COMMENT 'interface',
magic          INT     COMMENT 'data format',
partition      String  COMMENT 'partition format',
data           BINARY  COMMENT 'data',
ext_domain     Map<String,String>  COMMENT 'extension domain',
config_id      Int    COMMENT 'configuration id',
send_timestamp BIGINT  COMMENT 'send timestamp'
) PARTITIONED BY (dt BIGINT COMMENT 'partition');