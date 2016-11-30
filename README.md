# ECA
Test code for Efficient Correlation over Distributed Systems

1, The used METIS graph paritioner is avialble at (http://glaros.dtc.umn.edu/gkhome/metis/metis/download).

2, After exporting the jar file from the souce code, an sample of the job submission command is shown as below. Namely, besides the first four system parameters, there are 6 other parameters in our codes. There, "spark://taurusi5429:7077" is the sparkcontext, "hdfs://taurusi5429:54310/input/" is the file input path over HDFS, "32" is the number of executor cores, "Scm-250.csv.2" is the input event log, "0.1" is the value of \alpha and "0.5" is the value of \beta .
> spark-submit \ <br/>
  --class RF_Graph \ <br/>
  --master spark://taurusi5429:7077 \ <br/>
  --executor-memory 30G \ <br/>
  ECA.jar \ <br/>
  spark://taurusi5429:7077 \ <br/>
  hdfs://taurusi5429:54310/input/ \ <br/>
  32 \ <br/>
  Scm-250.csv.2 \ <br/>
  0.1 \ <br/>
  0.5 <br/>

3, How to set Spark, please refer to http://spark.apache.org/.

4, If any questions, please email to l.cheng(AT)tue.nl
