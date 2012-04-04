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
import java.lang.management.ManagementFactory;
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


public class DNSBackend {
	
	private static BufferedReader stdin;
	private static Resolver res;
	private static Properties config;
	private static List<DNSRecord> lastReturned;
	
	public static void main(String[] args)
	{
		
		// Conforms to PowerDNS PipeBackend protocol v1
		// Specified by: http://doc.powerdns.com/
		//     backends-detail.html#PIPEBACKEND-PROTOCOL
		
		// Load default configuration
        config = UpdateInfo.getDefaultConfiguration();
		
        // Setup Logging
        String suffix = ManagementFactory.getRuntimeMXBean().getName();
        String dir = config.getProperty("LOG_DIR");
        File outFile = new File(dir + "DNSBackend-" + suffix + ".log");
		
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
		stdin = new BufferedReader(new InputStreamReader(System.in));
		
		// Protocol HELO
		try {
			String helo = stdin.readLine();
			if (helo.equals("HELO\t1"))
				System.out.println("OK\t PowerDNS PipeBackend 1.0");
			else
				fail();
		} catch (Exception e) {
			fail();
			return;
		}
		
		lastReturned = null;
		
		// Begin answering queries
		while (true) {
			
			String query;
			try {
				query = stdin.readLine();
			} catch (Exception e) {
				fail();
				continue;
			}
			
			if (query == null) {
				return; // Quit on EOF
			}
			
			String[] queryTokens = query.split("\\s");
			Syslog.debug(DNSBackend.class, "Recieved: " + query);
			
			String queryType = queryTokens[0];
			if (queryType.equals("Q")) {
		
					
				if (queryTokens.length < 6) {
					fail();
					continue;
				}
				
				try {
					// For testing we allow record requests of the form
					// donar-test.[ip1].[ip2].[ip3].[ip4].real_record_fqdn
					// so test for that here...
					String[] fqdn = queryTokens[1].split("\\.");
					if ((fqdn[0].equals("donar-ip-test")) && (fqdn.length > 5)) {
						try {
							int ip1 = Integer.parseInt(fqdn[1]);
							int ip2 = Integer.parseInt(fqdn[2]);
							int ip3 = Integer.parseInt(fqdn[3]);
							int ip4 = Integer.parseInt(fqdn[4]);
							if ((ip1 <= 255) && (ip2 <= 255) && (ip3 <= 255) && (ip4 <= 255)) {
								String ipStr = fqdn[1] + "." + fqdn[2] + 
									"." + fqdn[3] + "." + fqdn[4];
								
								// Replace IP with simulated client
								query = query.replace(queryTokens[5], ipStr);
								
								// Replace test string
								query = query.replace("donar-ip-test." + ipStr + ".", "");
								Syslog.debug(DNSBackend.class, "Simulated query request for " +
										ipStr);
								Syslog.debug(DNSBackend.class, "New query: " + query);
							}
						}
						catch (NumberFormatException e) {
							continue;
						}
					}
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
					out += "END";
					Syslog.debug(DNSBackend.class, "Returned: " + out);
					System.out.println(out);
					
				} catch (IOException e) {
					fail();
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
				out += "END";
				Syslog.debug(DNSBackend.class, "Returned (AXFR): " + out);
				System.out.println(out);
			
			} else if (queryType.equals("PING")) {
				// Respond to ping
				end();
			
			} else {
			// Unknown query type
			fail();	
			}
		}	
	}
	
	
	private static void fail() {
		System.out.println("FAIL");
	}
	
	private static void end() {
		System.out.println("END");
	}
}
