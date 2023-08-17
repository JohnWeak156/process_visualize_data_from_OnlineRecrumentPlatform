# Run Spark job
### Go to the directory of project
```#cd data_recruitment_pipelines```
### Check wether data is updated on MySQL db
``` python ETL_spark_job/check_new_data.py"```

<img width="352" alt="image" src="../images/check_new_data.png">

### Update new data into MySQL db
``` python ETL_spark_job/export_newdata_to_mysql.py"```

