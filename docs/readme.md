# PySpark Review/demo/cookbook

## me
Brian Dewhirst
2023-11-15
b.dewhirst@gmail.com

## purpose
This project is intended to allow me to review (refresh) PySpark, create helper classes, etc.

Initially, it will likely be spartan/incomplete. I'll be doing a lot of refactoring, so it won't (obviously) match the original demo. (It follows it step by step, but makes it clear what's imported from where and doesn't run as a single 'notebook' style script.)

## incomplete disclaimers
Yes, PySpark really only makes sense if you're accessing a distributed processing system. This project may not practically utilize one. (TBD)

## quick references (the tutorial I'm following/paraphrasing)
https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark

### other tutorials to consider reviewing:
- https://www.guru99.com/pyspark-tutorial.html
- https://sparkbyexamples.com/pyspark-tutorial/

## tldr:
### install spark
- download spark (.tgz file) from [here](https://spark.apache.org/downloads.html) 
- extract to C:\Spark (Windows) or a subfolder of home (Linux/Unix) (i.e., {parent directory}/spark/spark-3.X.Y-bin-hadoop3/ ; extract both *.tgz and *.tar)
- On Windows, set environment variable (admin permission, `setx SPARK_HOME "C:\spark\spark-3.X.Y-bin-hadoop3"` then `setx PATH "C:\spark\spark-3.X.Y-bin-hadoop3\bin"`)
- (See above 'quick references' url for linux/mac instructions)

### now, returning to PyCharm/ the project "here":
- install pyspark (`python -m pip install pyspark` in an appropriate environment)
- install other minimal expected requirements (e.g., pandas, numpy etc.)
- start tutorial
- (resolve "java not found" error if necessary https://www.freecodecamp.org/news/how-to-install-java-on-windows-- apparent bug: JAVA_HOME should not end in /bin?)
- resume tutorial

### debugging notes:
- key debugging step was to add C:/.../java/jdk-vvv/bin <-- ***bin*** was the missing/critical detail
- possibly-optional/ possibly necessary step was setting environment variables at the System rather than User level
- possibly-optional-- I cleared out old/stale versions of oracle from System Path
- winutils isn't installed, which java warns about-- this tutorial doesn't mention it, others do-- something to keep in mind

### intent:
- follow the tutorial
- refactor the tutorial
- refactor again after it is done


### TODO:
- rather than all this spark=spark stuff, refactor into classes
- review / test user defined functions
- review 'ignite' framing
- review shipping environments-- how is that done these days?

### Final thoughts (for now)
- I've used this tutorial to refresh on PySpark and to confirm I've got it running locally