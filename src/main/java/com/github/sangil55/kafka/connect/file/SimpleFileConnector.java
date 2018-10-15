package com.github.sangil55.kafka.connect.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class SimpleFileConnector  extends SourceConnector {
	public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";
    public static final String OFFSETPATH_CONFIG = "offsetpath";
    public static final String BUFFERSIZE_CONFIG = "buffer.size";
    public static final String SLEEPTIME_CONFIG = "sleep.ms";
    public static final String CONNECTORKEY_CONFIG = "connector.key";
    private String connectorkeyname;
	private String filename;
	private String topic;
	private long BUFFER_SIZE = 10000;
    private String offsetpath="/tmp/";
    private int SLEEP_TIME = 0;
	 
	  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FILE_CONFIG, Type.STRING, null, Importance.HIGH, "Source pathname. If not specified, the standard input will be used")
      .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to")
	  .define(OFFSETPATH_CONFIG, Type.STRING, null, Importance.HIGH, "OFFSETFILE set default to /tmp/simplefileconnecor/")
      .define(BUFFERSIZE_CONFIG, Type.STRING, Importance.HIGH, "File Stream buffersize by polling")
	  .define(SLEEPTIME_CONFIG, Type.STRING, Importance.HIGH, "Thread sleep time")
	  .define(CONNECTORKEY_CONFIG, Type.STRING, Importance.HIGH, "Connector Key for Partiton, if key == \"FILE\" it will divide file to serveral partitons ");
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return "1.0";
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
		  filename = props.get(FILE_CONFIG);
		  topic = props.get(TOPIC_CONFIG);
		  connectorkeyname = props.get(CONNECTORKEY_CONFIG);
		  offsetpath = props.get(OFFSETPATH_CONFIG);
		  if(props.get(BUFFERSIZE_CONFIG)!= null)
			  BUFFER_SIZE = Long.parseLong(props.get(BUFFERSIZE_CONFIG));
		  if(props.get(SLEEPTIME_CONFIG) != null)
			  SLEEP_TIME = Integer.parseInt(props.get(SLEEPTIME_CONFIG));  
	}

	@Override
	public Class<? extends Task> taskClass() {
		// TODO Auto-generated method stub
		return SimpleFileTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// TODO Auto-generated method stub
		
		  ArrayList<Map<String, String>> configs = new ArrayList<>();
		  // Only one input partition makes sense.
		  Map<String, String> config = new HashMap<>();
		  if (filename != null)
		    config.put(FILE_CONFIG, filename);
		  config.put(TOPIC_CONFIG, topic);
		  if (offsetpath != null)
			    config.put(OFFSETPATH_CONFIG, offsetpath);
		  //if (BUFFER_SIZE != null)
		  config.put(CONNECTORKEY_CONFIG, connectorkeyname);
		  config.put(BUFFERSIZE_CONFIG, String.valueOf(BUFFER_SIZE));
		  config.put(SLEEPTIME_CONFIG, String.valueOf(SLEEP_TIME));
		  configs.add(config);
		  return configs;
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ConfigDef config() {
		// TODO Auto-generated method stub
		return CONFIG_DEF;
	}

}
