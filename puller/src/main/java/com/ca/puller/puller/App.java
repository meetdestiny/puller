package com.ca.puller.puller;

import java.net.InetAddress;

public class App 
{

    public static String HOSTNAME = "";
    
	public static final String LOG_FOLDER = "/log/apache-tomcat/";

	public static void main( String[] args ) throws Exception
	{
		HOSTNAME = InetAddress.getLocalHost().getHostName();
		AccessLogMonitor accessLogMonitor = new AccessLogMonitor();  
        accessLogMonitor.monitorLogFolder(LOG_FOLDER);
       
	}

	
}
