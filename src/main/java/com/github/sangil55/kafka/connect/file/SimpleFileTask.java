package com.github.sangil55.kafka.connect.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.tools.JavaFileObject;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.commons.io.input.CountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFileTask extends SourceTask {

	private static final Logger log = LoggerFactory.getLogger(SimpleFileTask.class);
	public String version() {
		// TODO Auto-generated method stub
		return "1.0";
	}

	
	 private String filename;
	 private String pathname="/data01/m_input";
	 private int BUFFER_SIZE = 100000;
	 private String offsetpath="/tmp/";
	 private int SLEEP_TIME = 0;
	 private int START_POS =0;
	 String offsetFileName = "kafka_csi_offset.csv";
	 
	  private InputStream stream;
	  private String topic;
	  private MetaData metadata = null;
	  private long lasttime = 0;
	  private final Object syncObj1 = new Object();
	  public void start(Map<String, String> props) {
	    filename = justGetOrAddSlash(props.get(SimpleFileConnector.FILE_CONFIG));
	    pathname = justGetOrAddSlash(props.get(SimpleFileConnector.FILE_CONFIG));
	    //default filename = pathname > spool all file in the just right directory
	    // ex  /root/1.csv , /root/2.csv >> just set filename as /root/
	    topic = props.get(SimpleFileConnector.TOPIC_CONFIG);
	    if(props.get(SimpleFileConnector.BUFFERSIZE_CONFIG) != null)
	    	BUFFER_SIZE = Integer.parseInt(props.get(SimpleFileConnector.BUFFERSIZE_CONFIG));
	    if(props.get(SimpleFileConnector.OFFSETPATH_CONFIG) != null)
	    	offsetpath = justGetOrAddSlash(props.get(SimpleFileConnector.OFFSETPATH_CONFIG));
	    if(props.get(SimpleFileConnector.SLEEPTIME_CONFIG) != null)
	    	SLEEP_TIME = Integer.parseInt(props.get(SimpleFileConnector.SLEEPTIME_CONFIG));
	   // pathname = "d:/getter/input.vol1";
    
	    log.info(">Kafka Connector Task start ");
	    log.info(">>Kafka Connector start Option > file path = " + pathname + ",topic = " + topic + ",buffersize = " + BUFFER_SIZE);
	    log.info(">>Kafka Connector start Option > offsetpath = " + offsetpath + ",SLEEP TIME = " + SLEEP_TIME);
	    
	    metadata = new MetaData(offsetpath);	
	  }
	  
	
	 public boolean isFinished(String str)
	 {
		 File file = new File(str);
		 long size = file.length();
		 
		 Long[] offset_full = metadata.offsetmap.get(file.getName());
//		 log.info("exact file size : " + size +" ,offset file size : "+offset_full[1] + " , offset = " + offset_full[0]);
		 
		 //size != pre file size    >> that mean file has changed
		 
		 if( !offset_full[1].equals(size) )
		 {
		 	 return false;
		 }
		 // offset != filesize  >> not finished or file has changed
		 if(!offset_full[1].equals(offset_full[0]))
		 {
		   	return false;
		 }
		 return true;
	 }
	
	 public String justGetOrAddSlash(String str)
	 {
		 if(str.charAt( str.length()-1 ) != '/')
			 str = str + '/';
		 return str;
	 }
	 
	 public int findOffsetUntilNewLine(CountingInputStream cin, long offset) throws IOException
	 {
		// System.out.println("find additional offset start");
		 int s;
		 cin.skip(offset);
		 byte[] b = new byte[1000];
		 if ((s = cin.read(b, 0, 1000)) == -1)
			 return 0;
		 
		 if(s<1000)
			 return s;
		 
		 String newstr = new String(b, "UTF-8");
		 int sumidx = 0;
		 while(true)
		 {
			// System.out.println("check for string : " + newstr);
			 int lindex = newstr.indexOf('\n');
			 if(lindex == -1)
			 {
				 return 1000 + findOffsetUntilNewLine(cin,1000);
			 }
			 if(lindex+1 == newstr.length())
				 return 1000 + findOffsetUntilNewLine(cin,1000);
			 if(newstr.charAt(lindex+1) == '[')
				 return sumidx+ lindex+1;
			 
			 if(lindex+2 == newstr.length() && newstr.charAt(lindex+1) == '\0')
				 return sumidx + lindex+3;
			 else if(lindex+2 == newstr.length())
				 return 1000 + findOffsetUntilNewLine(cin,1000);
			 if(newstr.charAt(lindex+1) == '\0' && newstr.charAt(lindex+2) == '\n')
				 return sumidx+lindex+3;
			 else{
				 //  0 12345
				 //  \n[aaaa
				 sumidx = sumidx + lindex+1;
				 newstr = newstr.substring(lindex+1);
			 }
		 }		 
		 
	 }
	 
	 public String getDate(String str)
	 {	
		 //MAS04_20180726_005638.log
		 String patternStr = "\\d{8}";
		 //if(str.split(regex))
		 Pattern pattern = Pattern.compile(patternStr);
         Matcher matcher = pattern.matcher(str);

         int count = 0;
         while (matcher.find()) {
        	 System.out.println(matcher.group());
            return matcher.group();
         }
         return "";
	 }

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		synchronized (this)
			{
			Thread.sleep(SLEEP_TIME);	
		
			// TODO Auto-generated method stub
			//log.info("polling-csi");
			
			File dirFile=new File(pathname);
			
			File []fileList=dirFile.listFiles();
			Arrays.sort(fileList);
			
			File metafile = new File(offsetpath+offsetFileName);
		    
	        final List<SourceRecord> results = new ArrayList<>();
	
			if(fileList==null)
				return null;
			if( metafile.exists() )
			{
				if(metadata == null)
					metadata = new MetaData(offsetpath);	
				metadata.ReadMetaFile(offsetpath+offsetFileName);
				//log.info("[INFO] meta file read done");
			}
			else
			{
				metadata = new MetaData(offsetpath);
			//	metadata.refresh(pathname);
			}
			for(int i = START_POS; i<fileList.length; i++)
			{
				log.info("PROGRESS -- TOTAL FILE Counts :" +i + "/" + fileList.length);
				if(fileList[i].isDirectory())
					continue;
				String filestr = pathname + fileList[i].getName();
				if(metadata.offsetmap.get(fileList[i].getName()) == null)
				{
				//	log.info("FILE REAED START WITH : " + pathname + fileList[i].getName() + "  TOTAL FILE Counts :" +i + "/" + fileList.length);
					
				//	log.info("------------------------------------------------no offset data read start");
					// new metadata should be created
					long filelen = fileList[i].length();					
					long offset = 0;
					CountingInputStream cin = null;
					CountingInputStream ctemp = null;
				
					try {
						cin = new CountingInputStream(new FileInputStream(filestr));
						ctemp = new CountingInputStream(new FileInputStream(filestr));
						int s;						
						int plusbytes = findOffsetUntilNewLine(ctemp, offset+BUFFER_SIZE);
						ctemp.close(); 	ctemp = null;						
						cin.skip(offset);
						int NEW_BUFFER_SIZE = BUFFER_SIZE + plusbytes; 
						byte[] b = new byte[NEW_BUFFER_SIZE];
						if ((s = cin.read(b, 0, NEW_BUFFER_SIZE)) != -1) {
							String newstr = new String(b, "UTF-8");
							String header = "<<HEADER>>date="+ getDate(fileList[i].getName()) + ",<</HEADER>>\n";
							
					        log.info("num of data bytes : " + s + "   ||  data : " /*+newstr*/);
					        Map sourcePartition = Collections.singletonMap("filename", filestr);
					        offset += s;
					        if(newstr.charAt(0) == '\0' && newstr.charAt(1) == '\n')
					        {
					        	log.info("Error case 'NULL+NEWLINE' at first");
					        	if(newstr.length()>2)
					        		newstr.substring(2);
					        }			
					        	
					        
					        Map sourceOffset = Collections.singletonMap("position", offset);
					        results.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, header+ newstr));						      
						 }
						Long[]ll = new Long[2];
						ll[0] = offset; ll[1] = filelen;						
						metadata.offsetmap.put(fileList[i].getName(),  ll);
						
					    metadata.saveoffset(offsetpath);
					    return results;  
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					finally{
						if(cin!=null)
							try {
								cin.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}
					
					
				}
				else
				{
					if(isFinished(filestr) == true)
					{
					//	log.info("FILE has done " + filestr);
						START_POS = i;
						continue;
						//to avoid reading already done file;						
					}
					else
					{
					//	log.info("FILE REAED START WITH : " + pathname + fileList[i].getName() + "  TOTAL FILE Counts :" +i + "/" + fileList.length);
						
						// start to read by 1000 rows
						
						long filelen = fileList[i].length();					
						long offset = 0;
						CountingInputStream cin = null;
						CountingInputStream ctemp = null;
						if( metadata.offsetmap.get(fileList[i].getName()) == null)
							offset = 0;
						else
							offset = metadata.offsetmap.get(fileList[i].getName())[0];
				//		log.info("--------------------------------not finished with offset, Read start : " + offset + "/" + filelen);
						try {
							cin = new CountingInputStream(new FileInputStream(filestr));
							ctemp = new CountingInputStream(new FileInputStream(filestr));
							int s;						
							int plusbytes = findOffsetUntilNewLine(ctemp, offset+BUFFER_SIZE);			
							ctemp.close(); 	ctemp = null;
							int NEW_BUFFER_SIZE = BUFFER_SIZE + plusbytes;
							byte[] b = new byte[NEW_BUFFER_SIZE];
							cin.skip(offset);
							if ((s = cin.read(b, 0, NEW_BUFFER_SIZE)) != -1) {
								String newstr = new String(b, "UTF-8");
								String header = "<<Header>>date="+ getDate(fileList[i].getName()) + "<</HEADER>>\n";
						        log.info("num of data bytes : " + s + "   ||  data : "/* +newstr */);
						        Map sourcePartition = Collections.singletonMap("filename", filestr);
						        offset += s;
						        
						        Map sourceOffset = Collections.singletonMap("position", offset);
						        results.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, header+newstr));						      
							 }
							Long[]ll = new Long[2];
							ll[0] = offset; ll[1] = filelen;						
							metadata.offsetmap.put(fileList[i].getName(), ll);
							
							metadata.saveoffset(offsetpath);
							return results;
							      
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						finally{
							if(cin!=null)
								try {
									cin.close();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
						}						
					}				
				}				
			}
			
			return results;
		}		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}	

}
