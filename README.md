# HybridM
Test code for Scalable Discovery of Hybrid Process Models. The relevant paper has been accepted by IEEE Transactions on Services Computing at http://dx.doi.org/10.1109/TSC.2019.2906203

0, The discovered models from BPIC 2017 are in the folder "Models".

1, The exported jar is available and also can be exported by the source codes.

2, With the jar file, the implementation contains two jobs as below:

2.1 The first is to discovery a DFG from logs. An sample of the job submission command is shown as below. Namely, besides the first four system parameters, there are 6 other parameters in our codes. There, "spark://taurusi5033:7077" is the sparkcontext, "hdfs://taurusitaurusi5033:54310/input/" is the file input path over HDFS, "64" is the number of executor cores, "application.tr" is the input event log, which is located in the HDFS at the path "hdfs://taurusi5033:54310/input/".

> spark-submit \ <br/>
  --class DFG \ <br/>
  --master spark://taurusi5033:7077 \ <br/>
  --executor-memory 15G \ <br/>
  /home/lcheng/HybridM/HybridM.jar \ <br/>
  spark://taurusi5033:7077 \ <br/>
  hdfs://taurusi5033:54310/input/ \ <br/>
  64 \ <br/>
  application.tr <br/>

2.2 The second is to discover places, replace with gateway, simplify gateways and output the dot file. An sample of the job submission command is shown as below. Different from above, there are three additional parameters, "/home/lcheng/EuroPar_17/order.m" is the output DFG by above job, "0.8" is the value of \alpha and "0.9" is the value of \beta .

> spark-submit \ <br/>
  --class HMD_2f \ <br/>
  --master spark://taurusi5033:7077 \ <br/>
  --executor-memory 15G \ <br/>
  /home/lcheng/HybridM/HybridM.jar \ <br/>
  spark://taurusi5033:7077 \ <br/>
  hdfs://taurusi5033:54310/input/ \ <br/>
  64 \ <br/>
  application.tr \ <br/>
  /home/lcheng/HybridM/ \ <br/>
  0.909 \ <br/>
  0.9 \ <br/>
  0.9 <br/>
  
  
3, How to set Spark, please refer to http://spark.apache.org/.

4, If any questions, please email to long.cheng(AT)ucd.ie
