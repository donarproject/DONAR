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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.protomatter.syslog.FileLog;
import com.protomatter.syslog.SimpleLogPolicy;
import com.protomatter.syslog.Syslog;

import donar.update.UpdateInfo;
import donar.update.util.CraqBackend;
import donar.update.util.DNSRecord;
import donar.update.util.MySQLBackend;
import donar.update.util.DONARBackend;


public class MysqlProximityResolver extends UnicastRemoteObject implements Resolver {
	private static final long serialVersionUID = 9156565886925806436L;
	private static Properties config;
	private static DONARBackend backend;
	private Connection dbConnection;
	private static final String MYSQL_SERVER_ADDR = "localhost";
	private static final String MYSQL_DB = "quova";
	private static final String MYSQL_USER = "nupserver";
	private static final String	MYSQL_PASS = "nupservertest";

	public MysqlProximityResolver() throws RemoteException {
        config = UpdateInfo.getDefaultConfiguration();
		try {
			if (config.getProperty("BACKEND", "CRAQ").equals("CRAQ")) {
				int craqPort = Integer.parseInt(config.getProperty("CRAQ_PORT"));
				String craqHost = config.getProperty("CRAQ_HOST");
				backend = new CraqBackend(craqHost, craqPort);
			}
			else {
				backend = new MySQLBackend();
			}
		} catch (Exception e) {
			System.out.println(e);
			return;
		}
		
		// Get MySQL Connection
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			// Passes string of the form
			// jdbc:mysql://127.0.0.1/pdns/user=nupserver&password=nupservertest
			dbConnection = DriverManager.getConnection("jdbc:mysql://" + 
					MYSQL_SERVER_ADDR + "/" + MYSQL_DB + "?user=" + 
					MYSQL_USER + "&password=" + MYSQL_PASS);
			
			Syslog.info(this, "Established connection to MYSQL on " +
				MYSQL_SERVER_ADDR);
		}
		catch (Exception e) {
			Syslog.log(this, e, Syslog.FATAL);
		}
		
	}
	
	public void printResultList(List<DNSRecord> results) {
		System.out.print(System.currentTimeMillis() + ": ");
		System.out.print("Returned: ");
		for (DNSRecord r: results) {
			System.out.print(r.content + " ");
		}
		System.out.println();
	}
	
	public List<DNSRecord> answerQuery(String query) throws RemoteException {
		System.out.print(System.currentTimeMillis() + ": ");
		System.out.println(query);
		String[] queryTokens = query.split("\\s");
		List<DNSRecord> results = null;
		
		try {
			results = backend.answerQuery(
				queryTokens[1],
				queryTokens[2],
				queryTokens[3],
				queryTokens[4],
				queryTokens[5]);
		}
		catch (IOException e) {
			throw new RemoteException();
		}
		if (queryTokens[3].equals("A") || 
				queryTokens[3].equals("ANY")) {

			Statement stmt;
			ResultSet rs;
			long refLong, refLat;
			try {
			  // Try to geolocate the client
				stmt = dbConnection.createStatement();
				rs = stmt.executeQuery("SELECT * FROM quova a WHERE "+
				"a.start_ip_int = (SELECT MAX(start_ip_int) FROM quova b " +
				"WHERE b.start_ip_int <= INET_ATON('" + queryTokens[5] + "" +
				"')) AND a.end_ip_int >= INET_ATON('" + queryTokens[5] + "')");
				
				if (!rs.next()) {
					printResultList(results);
					int numToAdd = 1;
					if (query.contains("measurement-lab.org")) {
					  String numToAddStr = config.getProperty("NUM_RECORDS_RETURNED", "3");
					  numToAdd = Integer.parseInt(numToAddStr);
					}
					LinkedList<DNSRecord> out = new LinkedList<DNSRecord>();
					// TODO Make this random
					if (results.size() > numToAdd) {
						for (int i = 0; i < numToAdd; i++) {
							out.add(results.get(i));
						}
						return out;
					}
					return results;
				}
				
				refLong = rs.getLong("longitude");
				refLat = rs.getLong("latitude");
				rs.close();
			} catch (SQLException e) {
				// Can't do geolocation, so just give all
				printResultList(results);
				return results;
			}
		  
			
			TreeMap<Double, Stack<DNSRecord>> map = 
				new TreeMap<Double, Stack<DNSRecord>>();
			LinkedList<DNSRecord> newResults = new LinkedList<DNSRecord>();
			
			for (DNSRecord r: results) {
				if (r.type.equals("A"))
					newResults.add(r);
			}
			
			// Remove A records and process seperately
			for (DNSRecord r: newResults) {
				results.remove(r);
				ResultSet recRs = null;
				
				Syslog.debug(this, "Trying to geolocated record " + r.content);
				// Try to geo-locate each record
				try {
					recRs = stmt.executeQuery("SELECT * FROM quova a WHERE "+
					"a.start_ip_int = (SELECT MAX(start_ip_int) FROM quova b " +
					"WHERE b.start_ip_int <= INET_ATON('" + r.ip.toString().split("/")[1] + "" +
					"')) AND a.end_ip_int >= INET_ATON('" + r.ip.toString().split("/")[1] + "')");
					if (!recRs.next()) {
						if (!map.containsKey(Double.POSITIVE_INFINITY)) {
							map.put(Double.POSITIVE_INFINITY, new Stack<DNSRecord>());
						}
						map.get(Double.POSITIVE_INFINITY).push(r);
						continue;
					}
				}
				catch (SQLException e) {
					if (!map.containsKey(Double.POSITIVE_INFINITY)) {
						map.put(Double.POSITIVE_INFINITY, new Stack<DNSRecord>());
					}
					map.get(Double.POSITIVE_INFINITY).push(r);
					continue;
				}
		
				try{
					long testLong = recRs.getLong("longitude");
					long testLat = recRs.getLong("latitude");
					
					// Get geo-distance in km
					double distance = Distance.distance(refLong, refLat, testLong, testLat, 'K');
					// Add or subtract .1 km to randomize collocated servers
					distance = distance + .1*(new Random()).nextDouble();
					
					//double distance = Math.pow(refLong - testLong, 2) +
					  Math.pow(refLat - testLat, 2);
					if (!map.containsKey(distance)) {
						map.put(distance, new Stack<DNSRecord>());
					}
					map.get(distance).push(r);
					
					Syslog.debug(this, "Adding record " + r.content + " with distance " + distance);
					recRs.close();
					continue;
				}
				catch (SQLException e) {
					if (!map.containsKey(Double.POSITIVE_INFINITY)) {
						map.put(Double.POSITIVE_INFINITY, new Stack<DNSRecord>());
					}
					map.get(Double.POSITIVE_INFINITY).push(r);
					continue;
				}
			}
			
			try {
				stmt.close();
			}
			catch (SQLException e) {}
			
			// Now add back closest NUM_RECORDS_RETURNED a-records
			int numAdded = 0;
			int numToAdd = 1;
			if (query.contains("measurement-lab.org")) {
			  String numToAddStr = config.getProperty("NUM_RECORDS_RETURNED", "3");
			  numToAdd = Integer.parseInt(numToAddStr);
			}
			
			// Randomize the order of co-located nodes
			for (double dist : map.keySet()) {
			  if (map.get(dist).size() > 1) {
			    Collections.shuffle(map.get(dist));
			  }
			}
			
			while (!map.isEmpty() && (numAdded < numToAdd)) {
			  
			  double closestDist = map.firstKey();
				Stack<DNSRecord> toAdd = map.get(closestDist);
				map.remove(closestDist);
				
				// Add results at this distance
				while ((numAdded < numToAdd) && !toAdd.isEmpty()) {
					results.add(toAdd.pop());
					numAdded++;
				}
			}
		}
		printResultList(results);
		return results;
	}
	
	public static void main (String args[]) throws IOException {
		MysqlProximityResolver res = null;
		String idName = null;
		Registry registry;
		
		try {
			registry = LocateRegistry.createRegistry(21002);
		}
		catch (Exception e) {}
		
		// Try to bind service
		try {
			idName = "Resolver";
			res = new MysqlProximityResolver();
			registry = LocateRegistry.getRegistry("localhost", 21002);
			registry.rebind(idName, res);
		}
		catch (RemoteException e) {
			System.out.println("Error registering resolver...");
			e.printStackTrace();
			return;
		}
		
		// Set up option list
		OptionParser parser = new OptionParser();
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "c", "config-file" } ),
				"Configuration file path.")
				.withRequiredArg().ofType(String.class);
		
        config = UpdateInfo.getDefaultConfiguration();
		
		OptionSet options = parser.parse( args );
		
		if (options.has("config-file")) {
         	String configFile = (String) options.valueOf("config-file");
            FileInputStream configFileStream = new FileInputStream(configFile);
         	config.load(configFileStream);
         }
		
		//		 Setup Logging
		//   We have two loggig channels, one is logging for development 
		//   purposes (DEFAULT CHANNEL) one is logging for information
		//   collecting purpose when deployed (INFO CHANNEL).
		
        String dir = config.getProperty("LOG_DIR");
        File outFile = new File(dir + "MysqlResolver.log");
		
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
		
		System.out.println("Resolver started...");

	}

}
