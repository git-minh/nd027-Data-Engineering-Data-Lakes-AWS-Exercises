CREATE EXTERNAL TABLE `steptrainer_landing`(
  `sensorreadingtime` timestamp, -- Assuming the original bigint represents a Unix timestamp
  `serialnumber` string, 
  `distancefromobject` double) -- Assuming the distance could require decimal precision
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'ignore.malformed.json'='TRUE') -- Set to TRUE to allow for resilience against malformed records
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/steptrainer/landing'
TBLPROPERTIES ('classification'='json');
