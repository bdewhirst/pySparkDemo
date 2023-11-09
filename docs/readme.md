# PySpark Review/demo/cookbook

## me
Brian Dewhirst
2023-11-09
b.dewhirst@gmail.com

## purpose
This project is intended to allow me to review PySpark, create helper classes, etc.

Initially, it will likely be spartan/incomplete

## incomplete disclaimers
Yes, PySpark really only makes sense if you're accessing a distributed processing system. This project may not practically utilize one. (TBD)

## quick references (the tutorial I'm following/paraphrasing)
https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark

## tldr:
### install spark
- download spark (.tgz file) from [here](https://spark.apache.org/downloads.html) 
- extract to C:\Spark (Windows) or a subfolder of home (Linux/Unix) (i.e., {parent directory}/spark/spark-3.X.Y-bin-hadoop3/ ; extract both *.tgz and *.tar)
- On Windows, set environment variable (admin permission, `setx SPARK_HOME "C:\spark\spark-3.X.Y-bin-hadoop3"` then `setx PATH "C:\spark\spark-3.X.Y-bin-hadoop3\bin"`)
- (See above 'quick references' url for linux/mac instructions)

### install pyspark (`python -m pip install pyspark` in an appropriate environment)