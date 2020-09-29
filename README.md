# Data Pre-processor

Simple Spark application to pre-process the data - anonymize the fields.

#### To run locally:

```
    ${SPARK_HOME}/bin/spark-submit --class "PreProcessor" --master local[4] target/scala-2.12/data-preprocessor_2.12-1.0.jar [<csvfile>]
```

 <csvfile> - csv file with columns "first_name", "last_name", "address" and "dat_of_birth"

              default value - "data_1mb.csv" - a csv file of size 1mb with the above 4 fields - random dataset generated using https://github.com/kavyabuvarma/datasetgen
