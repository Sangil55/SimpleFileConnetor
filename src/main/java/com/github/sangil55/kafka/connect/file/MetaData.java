package com.github.sangil55.kafka.connect.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;



public class MetaData
{
	//Key = file name , value = (offset,sizeoffile)
	public Map<String, Long[]> offsetmap = new HashMap<>();
//	public Map<String, Long> filesizemap = new HashMap<>();
	String offsetFileName = "kafka_csi_offset.csv";
	String offsetPath = "/tmp/";
	String fullname ="/tmp/kafka_csi_offset.csv";
	public void saveoffset(String strpath) throws IOException
	{
		int retrycnt = 0;
		while(retrycnt ++ < 5)
		{
			File file =new File(strpath + offsetFileName); 
			if (file.exists() && file.isFile())
		  {
			  file.delete();
		  }
			Writer fstream = new OutputStreamWriter(new FileOutputStream(strpath + offsetFileName), StandardCharsets.UTF_8);
			BufferedWriter bw = new BufferedWriter(fstream);
			if(fstream == null || bw == null)
			{
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				continue;
			}
			else
			{
				offsetmap.forEach((k,v) -> System.out.println("flush meta file >> " + k + ","+v[0].toString()+","+v[1].toString()));
				for(Map.Entry<String, Long[]> entry : offsetmap.entrySet()) {
				    String key = entry.getKey();
				    Long[] value = entry.getValue();
				    bw.write(key + ","+value[0].toString()+","+value[1].toString() + "\n");
				}
				bw.flush();
				bw.close();
				fstream.close();
				break;
			}
		}	
		if(retrycnt == 5)
			System.out.println("[error] offset file write error");
	}
	
	 public void ReadMetaFile(String offsetfile){
		 String filestr = offsetfile;
		 try {
			BufferedReader in = new BufferedReader(new FileReader(filestr));
			String s;
			 while ((s = in.readLine()) != null) {
			    //    System.out.println("read meta FILE!!!!!!" + s);
			        String[] tokens = s.split(",");
			        if(tokens.length>=2)
			        {
			        	Long[] l = new Long[2];
			        	l[0] = Long.parseLong(tokens[1]);
			        	l[1] = Long.parseLong(tokens[2]);
			        	Long.parseLong(tokens[1]);
			        	
			        	offsetmap.put(tokens[0], l);
			        }
			      }
			      in.close();
			      
			      ////////////////////////////////////////////////////////////////
	 
		} catch (IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	 
	 public boolean isFinished(String str)
	 {
		 File file = new File(str);
		 long size = file.length();
		 
		 Long[] offset_full = offsetmap.get(file.getName());
		 //log.info("exact file size : " + size +" ,offset file size :  "+offset_full[1] + " , offset = " + offset_full[0]);
		 //size != pre file size    >> that mean file has changed
		 if(size != offset_full[1])
			 return false;
		 // offset != filesize  >> not finished or file has changed
		 if(offset_full[1] != offset_full[0])
		 	return false;
		 return true;
	 }
	 
	 public String getFullname()
	 {
		 fullname = offsetPath + offsetFileName;
		 return fullname;
	 }
	 
}
