package donar.update;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionException;
import java.io.*;
import java.net.*;
import java.util.*;
import com.protomatter.syslog.*;

import donar.update.util.*;

public class UpdateServer {
	
	// Maps from hexadecimal string of public key SHA-1 hash to
	// next expected sequence number 
	
	DONARBackend backend;
	static Properties config;
	static List<String> liveSockets;  // Arraylist of strings describing sockets
	                          // currently in use. String format is 
	                          // <ip addr>:<port>
	
	private static class ShutdownHandler extends Thread {
		public void run()
		{
			System.out.println("Terminating NUP server...");
		}
	}
	
	public static void main(String[] args) throws IOException {		
		// Set up option list
		OptionParser parser = new OptionParser();
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "h", "?", "help" } ),
				"Prints this help message");
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "c", "config-file" } ),
				"Configuration file path.")
				.withRequiredArg().ofType(String.class);
		
		try {
			
			// Set Ctrl+C handler
			Runtime.getRuntime().addShutdownHook(new ShutdownHandler()); 
			
			OptionSet options = parser.parse( args );
            if (options.has("help")) {
            	System.out.println("Usage: UpdateClient [options] -- " +
            			"operation subdomain rrtype rrdata");
                parser.printHelpOn( System.out );
                return;
            }

            liveSockets = new ArrayList<String>();
            config = UpdateInfo.getDefaultConfiguration();

            
            if (options.has("config-file")) {
            	String configFile = (String) options.valueOf("config-file");
                FileInputStream configFileStream = new FileInputStream(configFile);
            	config.load(configFileStream);
            }
            reloadServerList();
          
            Syslog.info(UpdateServer.class, "NUPServer started and listening" +
            		"for incoming connections.");
         
            // Setup Logging
            
    		//   We have two loggig channels, one is logging for development 
    		//   purposes (DEFAULT CHANNEL) one is logging for information
    		//   collecting purpose when deployed (INFO CHANNEL).
    		
            String dir = config.getProperty("LOG_DIR");
            File outFile = new File(dir + "UpdateServer.log");
    		
    		// Create file log
    		FileLog fileLog = new FileLog(outFile, false, true);
 
    		// Add policy that listens to default channel and accepts messages
    		// based on urgency.
    		SimpleLogPolicy policy = new SimpleLogPolicy();
    		policy.addChannel(Syslog.DEFAULT_CHANNEL);
    		policy.setLogMask(config.getProperty("LOG_LEVEL"));
    		fileLog.setPolicy(policy);
    		
    		Syslog.removeAllLoggers(); // Get rid of defaults
    		Syslog.addLogger(fileLog);
            
            // Reload configuration file continuously
            if (options.has("config-file")) {
            	while (true) {
	            	Thread.sleep(Long.parseLong(config.getProperty(
	            			"CONFIG_RELOAD_INTERVAL")) * 1000);
	            	//Syslog.debug(UpdateServer.class, 
	            	//		"	Reloading configuration file.");
	            	String configFile = (String) options.valueOf("config-file");
	                FileInputStream configFileStream = new FileInputStream(configFile);
	                System.out.println(configFileStream.toString());
	                System.out.println("read config");
	            	config.load(configFileStream);
	            	
	            	reloadServerList();
            	}
            }
            
		} catch ( OptionException ex ) {
            parser.printHelpOn( System.err );
            System.err.println( "====" );
            System.err.println( ex.getMessage() );
        } catch ( Exception e ) {
        	Syslog.log(UpdateServer.class, e, Syslog.FATAL);
        }
		
	}
	
	/*
	 * Reloads server list based on current configuration and starts new
	 * threads if necessary. This method is necessary because we allow dymanic
	 * changing of the current sockets on which we listen for updates.
	 */
	public static void reloadServerList() {
        // Parse list in configuration file of address:port pairs. For each
        // pair, create a new thread to listen for incoming NUP packets.
        String serverList = config.getProperty("SERVER_LIST");
        String[] pairs = serverList.split(",");
        for (String pair: pairs) {
        	// Thread already exists
        	if (liveSockets.contains(pair)) {
        		continue;
        	}
        	
        	// Otherwise create new thread
        	String[] parts = pair.split(":");
        	if (parts.length != 2) {
        		throw new IllegalArgumentException("Invalid line in" +
        				" config file: " + serverList);
        	}
        	try {
        		InetAddress addr = InetAddress.getByName(parts[0]);
        		int port = Integer.parseInt(parts[1]);
        		
        		ListenerThread listener = new ListenerThread(addr, port, liveSockets, config);
        		Thread listenerThread = new Thread(listener);
        		listenerThread.start();
        	}
        	catch (Exception e) {
        		e.printStackTrace();
        		throw new IllegalArgumentException("Invalid line in" +
        				" config file: " + serverList + e.getMessage());
        	}
        }
        liveSockets.clear();
        liveSockets.addAll(Arrays.asList(pairs));
	}
}
