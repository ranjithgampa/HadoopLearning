export HADOOP_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006"
sudo rm /etc/hadoop/conf/mapred-site.xml
sudo cp /etc/hadoop/conf/mapred-site.xml.debug /etc/hadoop/conf/mapred-site.xml

hadoop fs -rm -r /user/ranjith/testData/output 
hadoop jar ./build/libs/HadoopLearning-all.jar /user/ranjith/testData/input/piwikSmallExport.csv /user/ranjith/testData/output 

#hadoop jar ./build/libs/HadoopLearning-all.jar /user/ranjith/testData/input/piwikData.csv /user/ranjith/testData/output
#hadoop jar ./build/libs/HadoopLearning-all.jar Input.txt output

