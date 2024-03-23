CREATE EXTERNAL TABLE `customer_curated`(
  `serialnumber` string, 
  `sharewithpublicasofdate` timestamp, -- Assuming conversion to timestamp for human readability
  `birthday` date, -- Adjusting date format for consistency and analysis
  `registrationdate` timestamp, -- Assuming conversion to timestamp
  `sharewithresearchasofdate` timestamp, -- Assuming conversion to timestamp
  `customername` string, 
  `email` string, 
  `lastupdatedate` timestamp, -- Assuming conversion to timestamp
  `phone` string, 
  `sharewithfriendsasofdate` timestamp) -- Assuming conversion to timestamp
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/customer/curated/'
TBLPROPERTIES (
  'classification'='json');
