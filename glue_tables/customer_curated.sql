CREATE EXTERNAL TABLE `customer_curated`(
  `serialnumber` string COMMENT 'from deserializer',
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer',
  `birthday` string COMMENT 'from deserializer',
  `registrationdate` bigint COMMENT 'from deserializer',
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer',
  `customername` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `lastupdatedate` bigint COMMENT 'from deserializer',
  `phone` string COMMENT 'from deserializer',
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer',
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths'='serialNumber,shareWithPublicAsOfDate,birthday,registrationDate,shareWithResearchAsOfDate,customerName,email,lastUpdateDate,phone,shareWithFriendsAsOfDate')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-srrb/customer/curated/'
TBLPROPERTIES (
  'classification'='json')
  