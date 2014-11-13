package com.ca.puller.puller;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
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
					Thread.sleep(1);
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
		AccessLogReader accessLogReader = new AccessLogReader();
		File currentFile = new File(fileName);
		List<Map> logs = accessLogReader.getLogs(currentFile, lineNumber, lineNumber+ BATCHSIZE);
		System.out.println("Logs Count:" + logs.size());
		if( logs.size() == 0) {
			if(!checkDateChange(fileName)){
				
			}
			
		} else {
			dispatcher.dispatchAll(logs, "INDEXNAME");
			lineNumber += logs.size();
		}
		
		
		

		
		updateStatus(lineNumber,currentFile);
	}

	private boolean checkDateChange(String fileName) {
		String fileDate = extractDate(fileName);
		String date = new SimpleDateFormat("YYYY-MM-DD").format(new Date());
		return fileDate.equals(date);
	}


	private void updateStatus(int end, File currentFile) {
		try {
			RandomAccessFile raf = new RandomAccessFile("/tmp/logstatus","w");
			raf.setLength(0);
			raf.writeBytes(end + "," + currentFile.getAbsolutePath());
			raf.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getStatus() {
		try {
			if(!(new File("/tmp/logstatus").exists())) 
				return "";
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
		return fileName.substring(11, 20);
	}
}