CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
  sensorReadingTime bigint,
  serialNumber string,
  distanceFromObject int
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'paths' = 'sensorReadingTime,serialNumber,distanceFromObject'
)
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 
  's3://steptrainerlanding69/steptrainerlanding/'
TBLPROPERTIES (
  'classification' = 'json'
);
