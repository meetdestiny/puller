package com.ca.puller.puller;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SessionManager extends Thread{

	private static final String user = "app";
	private String host; //= "100.64.15.210";
	public static Integer BATCH_SIZE = 100;
	LogParser parser ; 
	Dispatcher dispatcher;
	SimpleDateFormat sdf = new SimpleDateFormat("YYYY.MM.dd");
	Integer start = 0;
	Integer processed = 0;

	Date date = new Date();

	Channel channel = null;
	BufferedReader lineReader = null;

	public SessionManager(String host) {
		this.host = host; 
		parser = new LogParser(host);
		dispatcher= new Dispatcher();

	}

	public void run() {


		try {
			start = parser.getCount(host);
			String command = parser.getCommand();
			System.out.println("Firing: " + command);

			String log = null;
			Integer processed = 0;
			String today = sdf.format(new Date());
			String indexName = "logstash-" + today;
			BufferedReader lineReader = reconnect();
			while((log = lineReader.readLine()) != null) {

				dispatcher.dispatch(parser.parse(log), indexName);

				processed++;

				if(Math.abs(processed) % BATCH_SIZE == 0 ) {
					parser.setCount( processed);
				}
			}	

		} catch (Exception e) {
			
			e.printStackTrace();
		} 
	}

	private BufferedReader reconnect() throws Exception {
		JSch jsch=new JSch();
		jsch.addIdentity("/Users/asing20/.ssh/id_rsa", "passphrase");
		Session session=jsch.getSession(user, host, 22);
		session.connect();

		Channel channel =  session.openChannel("shell");

		PipedInputStream pis = new PipedInputStream(2048*1024);
		lineReader = new BufferedReader(new InputStreamReader(pis));
		InputStream is = new ByteArrayInputStream(parser.getCommand().getBytes());
		OutputStream os;
		os = new PipedOutputStream(pis);

		channel.setInputStream(is);

		channel.setOutputStream(os);
		channel.connect();
		return lineReader; 
	}

	private void disconnect() {
		try {
			if( channel!= null)
				channel.disconnect();
		} catch(Exception ex) {
			System.err.println("Cannot discoonect from :" + host +" Channel is already closed");
		}

		try {
			if( lineReader != null) {
				lineReader.close();
			}
		} catch(Exception ex) {
			System.err.println("Cannot discoonect from :" + host +" Channel is already closed");
		}


	}
}
