CREATE EXTERNAL TABLE `customer_trusted`(
  `serialnumber` string, 
  `sharewithpublicasofdate` date, -- Assuming the data represents dates and conversions will happen prior or during ingestion
  `birthday` date, -- Assuming 'birthday' is properly formatted for a date type
  `registrationdate` date, -- Converted from bigint with the assumption of Unix epoch time that will be converted prior or during ingestion
  `sharewithresearchasofdate` date, -- Assuming date format
  `customername` string, 
  `email` string, 
  `lastupdatedate` date, -- Converted from bigint, assuming date handling pre-conversion
  `phone` string, 
  `sharewithfriendsasofdate` date) -- Assuming date format
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://sparkling-lakes/customer/trusted/'
TBLPROPERTIES (
  'classification'='json');
