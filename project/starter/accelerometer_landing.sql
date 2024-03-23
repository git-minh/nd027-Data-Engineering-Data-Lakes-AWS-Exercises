CREATE EXTERNAL TABLE `accelerometer_landing` (
  `user` string, 
  `timestamp` timestamp, -- Assuming the original bigint was a UNIX timestamp
  `x` float, 
  `y` float, 
  `z` float)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'mapping.timestamp'= 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ', -- Added for proper timestamp formatting, adjust accordingly
  'ignore.malformed.json'='TRUE') -- Changed to 'TRUE' for resilience to malformed JSON
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/accelerometer/landing'
TBLPROPERTIES ('classification'='json');
