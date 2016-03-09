sudo rm /etc/hadoop/conf/mapred-site.xml
sudo cp /etc/hadoop/conf/mapred-site.xml.nodebug /etc/hadoop/conf/mapred-site.xml

hadoop fs -rm -r /user/ranjith/testData/output 
hadoop jar ./build/libs/HadoopLearning-all.jar /user/ranjith/testData/input/piwikExport.csv /user/ranjith/testData/output 

#hadoop jar ./build/libs/HadoopLearning-all.jar /user/ranjith/testData/input/piwikData.csv /user/ranjith/testData/output
#hadoop jar ./build/libs/HadoopLearning-all.jar Input.txt output

