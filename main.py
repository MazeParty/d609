


# ASSUME all S3 directories are made and only the landing zone directories contain json data. Namely /customerlanding, /steptrainerlanding, and /accelerometerlanding

#from accelerometer_landing_to_trusted import clean_accel_data
#from customer_landing_to_trusted import clean_customer_data
#from step_trainer_trusted import clean_steptrainer_data
#from customer_trusted_to_curated import customer_trusted_to_curated
from machine_learning_curated import machine_learning_to_curated

#1 Make the SQL Database and SQL Landing Zone Tables executing via copy paste in AWS Athena 

#execute create_db.sql
#execute accelerometer_landing.sql
#execute customer_landing.sql
#execute step_trainer_landing.sql

#2 Cleans S3 Landing Zone data from /accelerometerlanding /customerlanding uploads to /accelerometertrusted /customertrusted Trusted Zone executing in AWS Glue

#clean_customer_data()
#clean_accel_data()

#3 Cleans S3 Trusted Zone data from /customertrusted and uploads to /customercurated Curated Zone executing in AWS Glue
#customer_trusted_to_curated()

#4 Cleans S3 Landing Zone data from /steptrainerlanding and uploads to /steptrainertrusted Trusted Zone executing in AWS Glue
#clean_steptrainer_data()

#5 Now all Landing Zones and Trusted Zones are complete. Now lets make /machinelearningcurated in S3 manually and populate the directory with the method below, executing in AWS Glue
#machine_learning_to_curated()

#7 Now we can query all of our tables. We made the Landing Zone tables manually, and then trusted zone and curated zone had tables automatically made by scripting inference
