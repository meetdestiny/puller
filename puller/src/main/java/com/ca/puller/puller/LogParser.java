package com.ca.puller.puller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LogParser {

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private static final String logDateFormat = "dd/MMM/yyyy:HH:mm:ssZ";
	String host; 
	public LogParser(String host) {
		this.host = host;
	}

	public Map<String,Object> parse(String log) {
		Map<String, Object> map = new HashMap<String, Object>();
		//System.out.println("Parsing Log:" + log);
		String items[] = log.split("[ ]");
		if( items.length <14) {
			return new HashMap();
		}

		if("/ca-app/node".equals(items[7]))
			return new HashMap();
		if("/".equals(items[7]))
			return new HashMap();

		String logDate = (items[4] + items[5]);
		logDate = logDate.substring(1, logDate.length()-1);
		Date logDateTime;
		try {
			//System.out.println("Parsing datetime:" + logDate);
			logDateTime = new SimpleDateFormat(logDateFormat).parse(logDate);
		} catch (ParseException e) {
			e.printStackTrace();
			logDateTime = new Date();
		}

		String protocol = items[8].substring(0, items[8].length() -1);

		map.put("lb", items[0] );
		map.put("clientIP", items[1] );
		map.put("userIn", items[2] );
		map.put("authUser", items[3] );
		map.put("logDate", logDateTime );
		map.put("method", items[6] );
		map.put("url", items[7] );
		map.put("protocol", protocol );
		map.put("status",  items[9]);
		map.put("bytes", Integer.parseInt(items[10] ));
		map.put("ctime", Integer.parseInt(items[11]) );
		map.put("ptime", Integer.parseInt(items[12] ));
		map.put("consumerId", items[13]);
		map.put("corelationId", items[14]);
		map.put("@timestamp", logDateTime);
		return map;

	}

	public String getCommand() {
		String today = sdf.format(new Date());
		return "tail -f /log/apache-tomcat/access_log"+today+".log  -n "+ getCount(host) + "\n";
	}


	public int getCount(String host)  {
		final String filename = "/tmp/"+ host; 
		if(!(new File(filename).exists())){
			System.out.println("File " + filename +" does not exist:");
			return Integer.MAX_VALUE;
		}
		BufferedReader br = null;
		try {
			br =new BufferedReader(new FileReader(filename)) ;
			String count= br.readLine();
			if(count!= null) {
				return Integer.parseInt(count);
			}else {
				setCount(0);
				return Integer.MAX_VALUE;
			}
		} catch (FileNotFoundException e) {
			return Integer.MAX_VALUE;
		} catch (IOException e) {
			return Integer.MAX_VALUE;
		}finally {
			if( br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void setCount(Integer count)   {
		System.out.println("Setting count to :" + count);
		final String filename = "/tmp/"+ host; 
		RandomAccessFile raf = null;
		try {

			raf = new RandomAccessFile(filename, "rw"); 
			raf.setLength(0);
			raf.write(String.valueOf(count).getBytes());
		}catch( IOException ex) {
			System.out.println("Could not set the SetCOunt to :" + count);
		} finally  {
			try {
				raf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
