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

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Properties;

import com.protomatter.syslog.*;

import donar.update.util.*;


public class ListenerThread implements Runnable  {

	private int port;			// Port to listen on
	private InetAddress laddr;	// Local address to bind to
	private DONARBackend backend;
	private List<String> liveSockets;
	Properties config;
	private String stringRep; // String representation
	private BackupStorage bs;
	
	
	public ListenerThread(InetAddress laddr, int port, List<String>  liveSockets, Properties config)
		throws IOException
	{
		this.port = port;
		this.laddr = laddr;
		this.config = config;
		String backendString = config.getProperty("BACKEND", "CRAQ");
		if (backendString.equals("CRAQ")) {
			this.backend = new CraqBackend(config.getProperty("CRAQ_HOST"), Integer.parseInt(
					config.getProperty("CRAQ_PORT")));
			this.bs = new BackupStorage();
		}
		else if (backendString.equals("MYSQL")) {
			this.backend = new MySQLBackend();
		}
		this.liveSockets = liveSockets;
		this.stringRep = laddr.getHostAddress() + ":" + port;
	}
	
	/*
	 * Listen on specified address and port, and wait for NUP requests. When
	 * request is received dispatch handler thread to deal with request.
	 */
	public void run()
	{
		Syslog.debug(this, "Beginning listener thread for " + stringRep);
		try {
			
			DatagramSocket socket = new DatagramSocket(port, laddr);
			Syslog.info(UpdateServer.class, "Listening for update requests on" +
				stringRep);
			while (true) {
				
		    	byte[] packetBuf = new byte[UpdateInfo.NUP_MAX_PACKET_LENGTH];
				DatagramPacket packet =
					new DatagramPacket(packetBuf, packetBuf.length);
				socket.receive(packet);

				/*
				if (!(liveSockets.contains(stringRep))) { 
					Syslog.info(this, "Thread listening on " + stringRep + 
							" is self terminating.");
					socket.close();
					return; // Kill this thread
				}
				*/
				
				// Start new handler thread when packet is received
				Syslog.debug(UpdateServer.class, "Received packet. " +
						"Spawning handler.");
				
				if (Thread.activeCount() > 100) {
				  Syslog.debug(UpdateServer.class, 
				      "Too many active threads, ignoring packet.");
				}
				else {
  				UpdateServerThread packetHandler =
  					new UpdateServerThread(packet, socket, backend, bs);
  				Thread packetHandlerThread = new Thread(packetHandler);
  				packetHandlerThread.start();
				}
			}
			
		} catch (IOException e) {
			Syslog.error(UpdateServer.class, "Error running listner thread:" +
					e.getMessage());
			return;
		}
	}
}
