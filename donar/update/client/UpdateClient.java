package donar.update.client;

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

// TODO: add validate support

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionException;
import java.io.*;
import java.util.*;


public class UpdateClient {
	
	// Misc. constants
	private static final int REPLY_TIMEOUT = 8000; // ms
	private static final int SEND_PACKET_TRIES = 3;
	
	// Client constants
	private static final String DEFAULT_KEY_PATH = ".";
	private static final String DEFAULT_SERVER_ADDRESS = "localhost";

	private static final String USAGE_STRING = "Usage: donar.update.client.UpdateClient -s" +
	" update_server [options] [DONAR Directives] ";
	
	public static void main(String[] args) throws IOException {
		
		// Set up option list
		OptionParser parser = new OptionParser();
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "h", "?", "help" } ),
				"Prints this help message");
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "k", "key" } ),
				"Name of key files (without .pub or .pvt extension)" +
				" to use; will offer to generate a new key pair with this" +
				" name if not found. Default: nupkey")
				.withRequiredArg().ofType(String.class);
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "n", "sequence-number" } ),
				"Sequence number to use for this request; if not provided, " +
				" will search for stored sequence number in .seq file" +
				" corresponding to the key used or as a last resort query " +
				" for the next expected sequence number.")
				.withRequiredArg().ofType(Long.class);
		parser.acceptsAll(
				Arrays.asList(
						new String[] { "s", "server" } ),
				"Address or hostname of the remote host to which update request should be" +
				" sent.")
				.withRequiredArg().ofType(String.class);
		
		try {
			OptionSet options = parser.parse( args );
            if (options.has("help")) {
            	System.out.println(USAGE_STRING);
                parser.printHelpOn( System.out );
                return;
            }
           
            // Load or generate key pair
            String keyPath;
            if (options.has("key"))
            	keyPath = (String)options.valueOf("key");
            else
            	keyPath = DEFAULT_KEY_PATH;  
           
            // Get server
            String server;
            if (options.has("server"))
            	server = (String)options.valueOf("server");
            else {
            	System.out.println(USAGE_STRING);
            	parser.printHelpOn( System.out );
            	return;
            }
        
            UpdateClientConnection conn = new UpdateClientConnection(server,
            		keyPath);
           
            // Get ready to read from stdin
            BufferedReader stdin = new BufferedReader(
            		new InputStreamReader(System.in));
            
            // If request was specified as arguments, process it, otherwise
            // wait for request from stdin pipe.
            boolean success;
            List argRequest = options.nonOptionArguments();
            if (argRequest.size() > 0) {
            	success = conn.sendUpdate(argRequest);
            	if (success) System.exit(0);
            	else System.exit(1);
            }
            else {
	            String line;
	            System.out.println("Ready to process DONAR directives. Please begin...");
	            while ((line = stdin.readLine()) != null) {
	            	conn.sendUpdate(Arrays.asList(line.split(" ")));
	            }
            }
	        	
        }
        catch ( OptionException ex ) {
            parser.printHelpOn( System.err );
            System.err.println( "====" );
            System.err.println( ex.getMessage() );
        }

	}	
}
