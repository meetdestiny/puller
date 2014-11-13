package com.ca.puller.puller;

import java.net.InetAddress;

public class App 
{

    public static String HOSTNAME = "";
    
	private static final Integer batchSize = 100; 
	public static final String LOG_FOLDER = "/log/apache-tomcat/";

	public static void main( String[] args ) throws Exception
	{
		HOSTNAME = InetAddress.getLocalHost().getHostName();
		AccessLogMonitor accessLogMonitor = new AccessLogMonitor();  
        accessLogMonitor.monitorLogFolder(LOG_FOLDER);
       
	}

	
}
