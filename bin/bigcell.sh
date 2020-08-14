source $SPARK_HOME/conf/spark-env.sh
export HADOOP_CLASS1=$HADOOP_HOME\share\hadoop\common\*
export HADOOP_CLASS2=$HADOOP_HOME\share\hadoop\common\lib\*
export HADOOP_CLASS3=$HADOOP_HOME\share\hadoop\mapreduce\*
export HADOOP_CLASS4=$HADOOP_HOME\share\hadoop\mapreduce\lib\*
export HADOOP_CLASS5=$HADOOP_HOME\share\hadoop\yarn\*
export HADOOP_CLASS6=$HADOOP_HOME\share\hadoop\yarn\lib\*
export HADOOP_CLASS7=$HADOOP_HOME\share\hadoop\hdfs\*
export HADOOP_CLASS8=$HADOOP_HOME\share\hadoop\hdfs\lib\*
export SPARK_CLASS=$SPARK_HOME/jars/*

export BIGCELL_CLASSPATH=$HADOOP_CLASS1:$HADOOP_CLASS2:$HADOOP_CLASS3:$HADOOP_CLASS4:$HADOOP_CLASS5:$HADOOP_CLASS6:$HADOOP_CLASS7:$HADOOP_CLASS8
export BIGCELL_CLASSPATH=$BIGCELL_CLASSPATH:$SPARK_CLASS
export BIGCELL_CLASSPATH=$BIGCELL_CLASSPATH:lib/*:conf/:target/*
export BIGCELL_CLASSPATH=$BIGCELL_CLASSPATH:`hadoop classpath`


java -classpath "$BIGCELL_CLASSPATH" tech.datarchy.bigcell.Main