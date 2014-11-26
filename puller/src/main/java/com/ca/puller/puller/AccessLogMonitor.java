package com.ca.puller.puller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AccessLogMonitor {
	Dispatcher dispatcher = new Dispatcher();


	private static int BATCHSIZE = 100 ; 


	public void monitorLogFolder(String logFolder) {
		Path myDir = Paths.get(logFolder);
		try {

			while (true) {
				processLogs();
				try {
					Thread.sleep(1000);
				} catch(Exception ex) {

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void processLogs() throws IOException {
		String status[] = getStatus().split("[,]");
		int lineNumber = Integer.parseInt(status[0]);
		String fileName = status[1];
		//System.out.println("LineNumber : " + lineNumber + " FileName:" + fileName);
		AccessLogReader accessLogReader = new AccessLogReader();
		File currentFile = new File(fileName);
		List<Map> logs = accessLogReader.getLogs(currentFile, lineNumber, BATCHSIZE);
		System.out.println("Logs Count:" + logs.size());
		if( logs.size() == 0) {
			if(checkDateChange(fileName)){
				String newFile = getNewFileName(currentFile);
				updateStatus(0,currentFile);
			}
			
		} else {
			dispatcher.dispatchAll(logs, getIndexName(currentFile.getAbsolutePath()));
			lineNumber += logs.size();
		}
	
		updateStatus(lineNumber,currentFile);
	}

	private String getNewFileName(File currentFile) {
		//log/apache-tomcat/access_log2014-11-13.log
		String fileDate = currentFile.getAbsolutePath().substring(29,39);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date currentFileDate = new Date();
		try {
			currentFileDate = sdf.parse(fileDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar c = Calendar.getInstance(); 
		c.setTime(currentFileDate); 
		c.add(Calendar.DATE, 1);
		currentFileDate = c.getTime();
		
		String newFileName = "/log/apache-tomcat/access_log" + sdf.format(currentFileDate) + ".log";
		return newFileName;
	}


	private boolean checkDateChange(String fileName) {
		String fileDate = extractDate(fileName);
		String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		return !fileDate.equals(date);
	}


	private void updateStatus(int end, File currentFile) {
		try {
			RandomAccessFile raf = new RandomAccessFile("/tmp/logstatus","rw");
			raf.setLength(0);
			raf.writeBytes(end + "," + currentFile.getAbsolutePath());
			raf.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//access_log2014-11-26.log
	private String getStatus() {
		try {
			if(!(new File("/tmp/logstatus").exists()))  {
				String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
				//System.out.println("date" + date);
				return "1,/log/apache-tomcat/access_log" + date+ ".log" ;
			}
			RandomAccessFile raf = new RandomAccessFile("/tmp/logstatus","r");
			String value  = raf.readLine();
			raf.close();
			return value; 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}
	
	private String extractDate(String fileName) {
		return fileName.substring(29,39);
	}
	
	private String getIndexName(String filename) {
		//logstash-%{+YYYY.MM.dd}
		String date = extractDate(filename).replace("-", ".");
		//System.out.println("Creating index with date:" + date);
		return "logstash-" + date;
	}
}