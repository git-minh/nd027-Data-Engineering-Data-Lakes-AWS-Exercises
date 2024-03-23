CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string, 
  `email` string, 
  `phone` string, 
  `birthday` date, -- Assuming the date is in 'YYYY-MM-DD' format; adjust if the format differs
  `serialnumber` string, 
  `registrationdate` date, -- Converted from bigint assuming Unix timestamp format
  `lastupdatedate` date, -- Converted from bigint assuming Unix timestamp format
  `sharewithresearchasofdate` date, -- Converted from bigint assuming Unix timestamp format
  `sharewithpublicasofdate` date, -- Converted from bigint assuming Unix timestamp format
  `sharewithfriendsasofdate` date) -- Converted from bigint assuming Unix timestamp format
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE', 
  'ignore.malformed.json'='TRUE') -- Changing to TRUE to help with data resilience
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/customer/landing'
TBLPROPERTIES ('classification'='json');
