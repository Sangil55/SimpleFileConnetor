"# SimpleFileConnetor" 

how to use the connector

1.set up kafka

2.get the path for kafka/bin  (Ex /usr/hdp/2.6.5.0-292/kafka/bin/)

3.pull the project - maven update - build


4.if success, copy the jar with the plugin path (Ex /root/plugin/)
5.export the [4]plugin path 
export CLASSPATH=/root/plugin/*


6.set up the config file conf/connect-standalone.properties conf/custom.properties
*example for custom connetor properties 
name=connector2
tasks.max=1
connector.class=SimpleFileConnector
topic=p
file=/data01/m_input/             
input.path=/data01/m_input/
offsetpath=/tmp/
buffer.size=100000

7. execute with kafka standalone
/usr/hdp/2.6.5.0-292/kafka/bin/connect-standalone.sh /usr/hdp/2.6.5.0-292/kafka/conf/connect-standalone.properties /usr/hdp/2.6.5.0-292/kafka/conf/custom.properties