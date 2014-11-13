package com.ca.puller.puller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class AccessLogReader {
	
	LogParser logParser = new LogParser("");
	File logFile ; 
	public AccessLogReader() {
		
	}
	
	public List<Map> getLogs( int start, int end) throws IOException {
	   return getLogs(logFile, start, end);
	}

	public List<Map> getLogs(File logFile, int start, int end) throws IOException {
		FileInputStream inputStream = null;
		Scanner sc = null;
		List<Map> logs = new ArrayList(end-start+1);
		try {
			inputStream = new FileInputStream(logFile);
			sc = new Scanner(inputStream, "UTF-8");
			int i=1;
			while (sc.hasNextLine()) {
				System.out.println("Reading Line:" + i++) ;
				
				if( i >= start && i <= end )  {
					String line = sc.nextLine();
					logs.add(logParser.parse(line));
				}
				
				if ( i > end ) {
					break;
				}
			}
			if (sc.ioException() != null) {
				throw sc.ioException();
			}

		}catch (Exception ex) {
           ex.printStackTrace();
		}finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}
		}
		return logs;
	}

}
