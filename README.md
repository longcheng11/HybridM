# HybridM
Test code for Scalable Discovery of Hybrid Process Models


1, After exporting the jar file from the souce code. The implementation contains two jobs as below:

1.1 The first is to discovery a DFG from logs. An sample of the job submission command is shown as below. Namely, besides the first four system parameters, there are 6 other parameters in our codes. There, "spark://taurusi5033:7077" is the sparkcontext, "hdfs://taurusitaurusi5033:54310/input/" is the file input path over HDFS, "64" is the number of executor cores, "bpi_80.tr" is the input event log.

> spark-submit \ <br/>
  --class DFG \ <br/>
  --master spark://taurusi5033:7077 \ <br/>
  --executor-memory 15G \ <br/>
  /home/lcheng/EuroPar_17/HybridM.jar \ <br/>
  spark://taurusi5033:7077 \ <br/>
  hdfs://taurusi5033:54310/input/ \ <br/>
  64 \ <br/>
  bpi_80.tr <br/>

1.2 The second is to discover places, replace with gateway, simplify gateways and output the dot file. An sample of the job submission command is shown as below. Different from above, there are three additional parameters, "/home/lcheng/EuroPar_17/order.m" is the output DFG by above job, "0.8" is the value of \alpha and "0.9" is the value of \beta .

> spark-submit \ <br/>
  --class HMD_2f \ <br/>
  --master spark://taurusi5033:7077 \ <br/>
  --executor-memory 15G \ <br/>
  /home/lcheng/EuroPar_17/HybridM.jar \ <br/>
  spark://taurusi5033:7077 \ <br/>
  hdfs://taurusi5033:54310/input/ \ <br/>
  64 \ <br/>
  bpi_80.tr \ <br/>
  /home/lcheng/EuroPar_17/order.m \ <br/>
  0.8 \ <br/>
  0.9 <br/>
  
  
2, How to set Spark, please refer to http://spark.apache.org/.

3, A prototye that can be integrated in ProM (http://www.promtools.org/) is in the process of development, and real demos with standard inputs (i.e., XES) and outputs are exeptated to be done before May.

4, If any questions, please email to l.cheng(AT)tue.nl