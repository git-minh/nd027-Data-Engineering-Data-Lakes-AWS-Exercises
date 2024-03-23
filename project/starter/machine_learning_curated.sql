CREATE EXTERNAL TABLE `machine_learning_curated`(
  `serialnumber` string, 
  `z` double,
  `timestamp` timestamp, -- Converted from bigint for better usability
  `y` double, 
  `user` string, 
  `x` double, 
  `distancefromobject` int)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/machinelearning/curated/'
TBLPROPERTIES (
  'classification'='json');
