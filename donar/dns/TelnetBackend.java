package donar.dns;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Properties;

import com.protomatter.syslog.FileLog;
import com.protomatter.syslog.SimpleLogPolicy;
import com.protomatter.syslog.Syslog;

import donar.update.UpdateInfo;
import donar.update.util.DNSRecord;


public class TelnetBackend {
	
	private static BufferedReader stdin;
	private static Resolver res;
	private static Properties config;
	private static List<DNSRecord> lastReturned;
	
	public static void main(String[] args) throws UnknownHostException, IOException
	{
		
		// Conforms to PowerDNS PipeBackend protocol v1
		// Specified by: http://doc.powerdns.com/
		//     backends-detail.html#PIPEBACKEND-PROTOCOL
		
		// Load default configuration
        config = UpdateInfo.getDefaultConfiguration();
		
        // Setup Logging
        String suffix = ManagementFactory.getRuntimeMXBean().getName();
        String dir = config.getProperty("LOG_DIR");
        File outFile = new File(dir + "TelnetBackend-" + suffix + ".log");
		
		// Create file log
		FileLog fileLog = new FileLog(outFile, false, true);

		// Add policy that listens to default channel and accepts messages
		// based on urgency.
		SimpleLogPolicy policy = new SimpleLogPolicy();
		policy.addChannel(Syslog.DEFAULT_CHANNEL);
//		policy.setLogMask(config.getProperty("LOG_LEVEL"));
//		fileLog.setPolicy(policy);
		
		Syslog.removeAllLoggers(); // Get rid of defaults
		Syslog.addLogger(fileLog);	
		
		
		// Setup RPC connection
		try {
			Registry registry = LocateRegistry.getRegistry("localhost", 21002);
			res = (Resolver) registry.lookup("Resolver");
		}
		catch (RemoteException e) {
			System.out.println("Unable to connect to server...");
			e.printStackTrace();
			return;
		}
		catch (NotBoundException e) {
			System.out.println("Unable to connect to server...");
			e.printStackTrace();
			return;
		}
	
        config = UpdateInfo.getDefaultConfiguration();
        
		// Set up IO
        ServerSocket listen = new ServerSocket(21000);
        Socket accepted = listen.accept();
		stdin = new BufferedReader(new InputStreamReader(accepted.getInputStream()));
		OutputStream outStream = accepted.getOutputStream();
		// Protocol HELO
		try {
			String helo = stdin.readLine();
			if (helo.equals("HELO\t1")) {
				String out = "OK\t PowerDNS PipeBackend 1.0\r\n";
				outStream.write(out.getBytes());
			}
			else {
				String out = "END";
				outStream.write(out.getBytes());
			}
		} catch (Exception e) {
			String out = "END";
			outStream.write(out.getBytes());
			return;
		}
		
		lastReturned = null;
		
		// Begin answering queries
		while (true) {
			
			String query;
			try {
				query = stdin.readLine();
				System.out.println(query);
			} catch (Exception e) {
				String out = "END";
				outStream.write(out.getBytes());
				continue;
			}
			
			if (query == null) {
				return; // Quit on EOF
			}
			
			String[] queryTokens = query.split("\\s");
			Syslog.debug(TelnetBackend.class, "Recieved: " + query);
			
			String queryType = queryTokens[0];
			if (queryType.equals("Q")) {
		
					
				if (queryTokens.length < 6) {
					String out = "END";
					outStream.write(out.getBytes());
					continue;
				}
				
				try {
					List<DNSRecord> results = res.answerQuery(query);
					lastReturned = results;
					String out = "";
					for (DNSRecord record : results) {
						out += "DATA\t";
						out += queryTokens[1] + "\t";
						out += "IN\t";
						out += record.type + "\t";
						out += record.ttl + "\t";
						out += "1\t";
						out += record.content + "\r\n";
					}
					out += "END\r\n";
					Syslog.debug(TelnetBackend.class, "Returned: " + out);
					outStream.write(out.getBytes());
					
				} catch (IOException e) {
					String out = "END";
					outStream.write(out.getBytes());
					e.printStackTrace();
					continue;
				}
			} else if (queryType.equals("AXFR")) {
			// Hacky, just return last record again:
				String out = "";
				for (DNSRecord record : lastReturned) {
					out += "DATA\t";
					out += queryTokens[1] + "\t";
					out += "IN\t";
					out += record.type + "\t";
					out += record.ttl + "\t";
					out += "1\t";
					out += record.content + "\r\n";
				}
				out += "END\r\n";
				Syslog.debug(TelnetBackend.class, "Returned (AXFR): " + out);
				outStream.write(out.getBytes());
			
			} else if (queryType.equals("PING")) {
				// Respond to ping
				String out = "END";
				outStream.write(out.getBytes());
			
			} else {
			// Unknown query type
				String out = "END";
				outStream.write(out.getBytes());
			}
		}	
	}
}
