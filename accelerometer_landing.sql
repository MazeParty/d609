CREATE EXTERNAL TABLE `accelerometer_landing` (
  `user` string, 
  `timestamp` bigint, 
  `x` double, 
  `y` double, 
  `z` double
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'paths' = 'timestamp,user,x,y,z'
)
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://accelerometerlanding69/accelerometerlanding/'
TBLPROPERTIES (
  'classification' = 'json'
);
