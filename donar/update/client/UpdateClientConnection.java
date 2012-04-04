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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import donar.dns.attrs.DoubleListRecordAttribute;
import donar.dns.attrs.DoubleRecordAttribute;
import donar.dns.attrs.IntegerRecordAttribute;
import donar.update.UpdateInfo;
import donar.update.util.RequestElement;
import donar.update.util.Verifiers;


public class UpdateClientConnection {
	
	private String server;  	// DONAR server to talk to
	private String keyPath; 	// Directory to look for .pub/.pri/.seq files
	private KeyPair keyPair; 
	
	long seqNum;
	
	public class ReplyPacket {
		// Class to hold data for one reply packet
			
		/*
		 * Create a new ReplyPacket from the byte array. Throw an exception
		 * if reply[] does not describe a valid reply.
		 */
		public ReplyPacket(byte[] reply) throws IOException {
			if (reply == null) {
				throw new IOException("Got empty reply");
			}
			
			ByteArrayInputStream replyStream = new ByteArrayInputStream(reply);
			DataInputStream replyData = new DataInputStream(replyStream);
			
			try {
				
				// Magic string
				byte[] magic = new byte[UpdateInfo.NUP_MAGIC_STRING.length];
				replyData.readFully(magic);
				if (!Arrays.equals(magic, UpdateInfo.NUP_MAGIC_STRING))
					throw new IOException("Invalid magic string");
				
				// Version number
				short version = replyData.readShort();
				if (!(version == UpdateInfo.NUP_VERSION))
					throw new IOException("Invalid NUP version");
				
				// DSA public key
				short keylen = replyData.readShort();
				if (keylen > 0) {
					// Allow for keyless replies
					byte[] packetKey = new byte[keylen];
					replyData.readFully(packetKey);
					byte[] ourKey = keyPair.getPublic().getEncoded();
					if (!Arrays.equals(packetKey, ourKey))
						throw new IOException("Signature does not match");
				}
				
				// Sequence number
				long packetSequenceNum = replyData.readLong();
				if (!(packetSequenceNum == seqNum) && (packetSequenceNum > 0))
					throw new IOException("Sequence number is not correct. Found " +
							packetSequenceNum + " should be " + seqNum );

				// Packet is valid, so read other data
				nextSequenceNum = replyData.readLong();
				short reCount = replyData.readShort();
				packetStatusCode = replyData.readShort();
				elementStatusCodes = new short[reCount];
				for (int i = 0; i < reCount; i++) {
					elementStatusCodes[i] = replyData.readShort();
				}
				
			} catch (Exception e) {
				// Fail on exception
				throw new IOException("General failure creating ReplyPacket" +
						e.getMessage());
			}
			
		}
		
		public boolean isSuccess() {
			return (packetStatusCode == UpdateInfo.PACKET_SUCCESS);
		}
		
		public String toString() {
			// Don't print out individual RE statuses unless we failed
			if (packetStatusCode != UpdateInfo.PACKET_SUCCESS)
			{
				String out = "Request not successful:\n" + packetStatusCode;
				for (int i = 0; i < elementStatusCodes.length; i++) {
					out += "Request element " + i + ": ";
					out += UpdateInfo.getREDescription(
							elementStatusCodes[i]);
				}
				return out;
			}
			else 
				return "Packet succesfully processed";
			
		}
		
		long nextSequenceNum;
		short packetStatusCode;
		short[] elementStatusCodes;
	}
	
	/*
	 * Send an update to the server based on an array of string arguments.
	 * Propper formats below...
	 * 
	 * add [key1=val1] [key2=val2]
	 *   possible keys: donar-ttl=1000
	 *                  subdomain=foo.com
	 *                  type=A
	 *                  ttl=100
	 *                  data=1.2.3.4
	 *                  donar-split=.1
	 *                  donar-epsilon=.01
	 * delete subdomain=x [type=y] [data=z]
	 * validate domain=x soattl=y email=z
	 * 
	 *  Return true if reply is successful, otherwise throws an exception.
	 */
	public boolean sendUpdate(List<String> args) {
		try {
			List<RequestElement> requestElements = generateRequestElements(args);
			if (requestElements == null) {
				String argString = "";
				for (String arg : args) {
					argString = argString + arg + " ";
				}
				System.out.println("Could not interpret directive: " + argString);
				return false;
			}
		    byte[] packetData = buildPacket(requestElements);
		    ReplyPacket reply = sendPacket(packetData);
		    
		    if (reply != null)
		    	updateSequenceNum(reply.nextSequenceNum);
		    System.out.println("Reply was: " + reply);
		    return reply.isSuccess();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error sending update: " + e + e.getMessage());
			return false;
		}
	}
	
	/*
	 * Initialize a donar connection to server. keyPath is the local
	 * directory where public and private key files are stored (or where they
	 * will be created if no such files exist).
	 */
	public UpdateClientConnection(String server, String keyPath)
	{
		this.server = server;
		this.keyPath = keyPath + "/nupkey";
		this.keyPair = setupKeyPair();
		setSequenceNumber();
	}
	
	/* 
	 * If private and public key files exist at path, then return the KeyPair
	 * stored in those files. Otherwise, create a new key pair and store
	 * them in the path directory.
	 */
	private KeyPair setupKeyPair()
	{
		KeyPair kp;
		if (new File(keyPath + ".pub").exists() ||
        		new File(keyPath + ".pvt").exists()) {
        		// Attempt toload existing keypair
        		kp = loadDSAKeys(keyPath);
        		if (kp == null) {
        			System.err.println("Error: keys at " + keyPath + ".pub" +
        					" and " + keyPath + ".pvt could not be loaded.");
        		}
    	} else {
    		// No key pair with the given name; generate a new one
    		System.out.println("Generating new keys...");
    		kp = generateDSAKeys(keyPath);
    		if (kp == null) {
    			System.err.println("Error: key could not be generated at " +
    					keyPath + ".pub and " + keyPath + ".pvt.");
    		}
    	}
		
		return kp;
	}
	
	/*
	 * Generate a new private/public key pair and save key files in the path
	 * directory. Raturns null if KeyPair cannot be created or saved.
	 */
	private KeyPair generateDSAKeys(String path)
	{
		// Generate
		KeyPairGenerator kpg;
		try {
			kpg = KeyPairGenerator.getInstance("DSA");
		} catch (java.security.NoSuchAlgorithmException e) {
			System.err.println("Error: This system does not provide support "
					+ "for SHA512withDSA.");
			return null;
		}
		kpg.initialize(1024);
		KeyPair kp = kpg.generateKeyPair();
		
		// Save
		PublicKey pub = kp.getPublic();
		PrivateKey pvt = kp.getPrivate();
		try {
			FileOutputStream pubOut =
				new FileOutputStream(new File(path + ".pub"));
			FileOutputStream pvtOut =
				new FileOutputStream(new File(path + ".pvt"));
			pubOut.write(pub.getEncoded());
			pvtOut.write(pvt.getEncoded());
			pubOut.close();
			pvtOut.close();
		} catch (Exception e) {
			return null; // Don't return keypair if it cannot be saved
		}
		
		return kp;
	}
	
	
	/*
	 * Try to load the DSA keys from files in the path directory. Requires
	 * files that end in .pub and .pvt. Return KeyPair containing the data
	 * in those files.
	 */
	private KeyPair loadDSAKeys(String path)
	{
		// Load
		try {
			// Load
			File pubFile = new File(path + ".pub");
			File pvtFile = new File(path + ".pvt");
			DataInputStream pubIn =
				new DataInputStream(new FileInputStream(pubFile));
			DataInputStream pvtIn =
				new DataInputStream(new FileInputStream(pvtFile));
			byte[] pubData = new byte[(int)pubFile.length()];
			byte[] pvtData = new byte[(int)pvtFile.length()];
			pubIn.readFully(pubData);
			pvtIn.readFully(pvtData);
			pubIn.close();
			pvtIn.close();
			
			// Convert
			KeyFactory keyFactory = KeyFactory.getInstance("DSA");
			X509EncodedKeySpec pubSpec = new X509EncodedKeySpec(pubData);
			PKCS8EncodedKeySpec pvtSpec = new PKCS8EncodedKeySpec(pvtData);
			PublicKey pubKey = keyFactory.generatePublic(pubSpec);
			PrivateKey pvtKey = keyFactory.generatePrivate(pvtSpec);
			
			return new KeyPair(pubKey, pvtKey);
			
		} catch (Exception e) {
			// Fail out on any kind of I/O or key loading error
			return null;
		}
	}
	
	
	/* 
	 * Get the sequence number for this interaction. First look
	 * in keyPath for the file. Otherwise, query the server.
	 */
	private void setSequenceNumber() {
	// Look it up in .seq file first
	try {
		File seqFile = new File(keyPath + ".seq");
		FileInputStream seqIn = new FileInputStream(seqFile);
		DataInputStream seqData = new DataInputStream(seqIn);
		long sequenceNum = seqData.readLong();
		if (sequenceNum >= 0)
			updateSequenceNum(sequenceNum);
	} catch (Exception e) {
		// If file method didn't work for any reason, keep going
	
		// Not found in a file, query server
		try {
			
			byte[] queryPacket = // Standard query packet format
				buildPacket(new LinkedList<RequestElement>());
			if (queryPacket == null) {
				System.err.println("Error: could not build sequence" +
						" number request packet");
				return;
			}
			System.out.println("Querying for sequence number...");
			ReplyPacket reply =
				sendPacket(queryPacket);
			if (reply == null) {
				System.err.println("\nError: could not query server" +
				" for sequence number");
				return;
			}
			System.err.println("Got answer: " + reply.nextSequenceNum);
			updateSequenceNum(reply.nextSequenceNum);
		
		} catch (Exception ex) {
			// Give up on exception
			System.err.println("Error: could not query server" +
					" for sequence number");
			return;
		    }
		}
	}

	private  void updateSequenceNum(long newSequenceNum)
	{
		seqNum = newSequenceNum;
		try {
			File seqFile = new File(keyPath + ".seq");
			FileOutputStream seqOut = new FileOutputStream(seqFile, false);
			DataOutputStream seqData = new DataOutputStream(seqOut);
			seqData.writeLong(newSequenceNum);
			seqData.close();
		} catch (Exception e) {
			System.err.println("Error: could not save new sequence number");
			return;
		}
	}
	
	/*
	 * Build a NUP packet given a list of request elements to include
	 * in that packet.
	 */
	private byte[] buildPacket(
			List<RequestElement> requestElements) throws IOException {
		
		PublicKey pubKey = keyPair.getPublic();
		PrivateKey pvtKey = keyPair.getPrivate();
		byte[] pubKeyBytes = pubKey.getEncoded();
		
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		
		// Write header info
		dataOut.writeBytes("DONAR");
		dataOut.writeShort(UpdateInfo.NUP_VERSION);
		dataOut.writeShort(pubKeyBytes.length);
		dataOut.write(pubKeyBytes);
		dataOut.writeLong(seqNum);
		dataOut.writeShort(requestElements.size());
		
		// Write request elements
		for (RequestElement re : requestElements) {
			re.writeToStream(dataOut);
		}
		
		// Sign
		dataOut.flush();
		byte[] toSign = bytesOut.toByteArray();
		try {
			Signature sig = Signature.getInstance("SHA1withDSA");
			sig.initSign(pvtKey);
			sig.update(toSign);
			byte[] signature = sig.sign();
			dataOut.writeShort(signature.length);
			dataOut.write(signature);
		} catch (Exception e) {
			System.err.println("Error: could not sign packet");
			return null;
		}
		
		// Finish packet
		dataOut.flush();
		byte[] packet = bytesOut.toByteArray();
		if (packet.length > UpdateInfo.NUP_MAX_PACKET_LENGTH) {
			System.err.println("Error: packet too big. Discarding...");
			return null;
		}
		
		return packet;
		
	}
	
	private ReplyPacket sendPacket(byte[] packet) {

		// Break out port number
		StringTokenizer tokenizer = new StringTokenizer(server, ":");
		
		if (tokenizer.countTokens() > 2) {
			System.err.println("Error: invalid server: " + server);
			return null;
		}
		
		String serverAddr = tokenizer.nextToken();
		String serverPortStr;
		
		int serverPort;
		
		if (tokenizer.hasMoreTokens()) {
			serverPortStr = tokenizer.nextToken();
			try {
				serverPort = Integer.parseInt(serverPortStr);
			} catch (Exception e) {
				System.err.println("Error: invalid port: " + serverPortStr);
				return null;
			}
		} else {
			serverPort = UpdateInfo.NUP_DEFAULT_SERVER_PORT;
		}
		
		// Look up hostname
		ArrayList<InetAddress> addrs;
		try {
			addrs = new ArrayList<InetAddress>(
			        Arrays.asList(InetAddress.getAllByName(serverAddr)));
		} catch (Exception e) {
			System.err.println("Error: could not lookup server: " + server);
			return null;
		}
		
		Collections.shuffle(addrs); // Load balance amongst update servers
		
		// Set up UDP
		DatagramSocket dgSocket;
		ArrayList<DatagramPacket> dgPackets = new ArrayList<DatagramPacket>();
		
		try {
			dgSocket = new DatagramSocket();
			dgSocket.setSoTimeout(5 * 1000); // Five sec timeout
		} catch (Exception e) {
			System.err.println("Error: could not create socket");
			return null;
		}
		
		for (InetAddress addr : addrs) {
		    dgPackets.add(new DatagramPacket(
	            packet, packet.length, addr, serverPort));
		}
		
		// Send packet
		int tries;
		byte[] replyBuf = new byte[UpdateInfo.NUP_MAX_PACKET_LENGTH];
		byte[] replyData = null;
		DatagramPacket reply =
			new DatagramPacket(replyBuf, UpdateInfo.NUP_MAX_PACKET_LENGTH);
		boolean success = false;
		for (tries = 0; tries < 3; tries++) {
		  for (DatagramPacket dg : dgPackets) {
	        String addrNice = dg.getAddress().toString();
		    if (success) { continue; }
	        System.err.println("Attempting send to " + addrNice);
		    try {
			  dgSocket.send(dg); 
		    } catch (Exception e) {
		      System.err.println("Error: failed during packet send to " + addrNice);
		      continue;
		    }
		    
		    try {
                // Keep listening until corresponding reply packet
                // is received or timeout occurs
                dgSocket.receive(reply);
                replyData = new byte[reply.getLength()];
                for (int i = 0; i < reply.getLength(); i++) {
                    replyData[i] = reply.getData()[i];
                }
                success = true;
                break;  
            } catch (SocketTimeoutException e) {
                System.err.println("Error: timed out receiving from " + addrNice);
            } catch (Exception e) {
                System.err.println("Error: unknown error receiving from " + addrNice);
                e.printStackTrace();
                return null;
            }
            
		  }   
		}
		if (!success) {
		    System.err.println("Error: could not process packet on any update server");
	        return null;
		}


	    try {
	        return new ReplyPacket(replyData);
		}
		catch (Exception e) {
			System.err.println("Error parsing reply packet: " + 
			    e.getMessage());
			return null;
		}
	}
	
	/*
	 * Generate request elements from a list of arguments seperated by spaces.
	 * 
	 * add [key1=val1] [key2=val2]
	 *   possible keys: donar-ttl=1000
	 *                  subdomain=foo.com
	 *                  type=A
	 *                  ttl=100
	 *                  data=1.2.3.4
	 *                  donar-split=.1
	 *                  donar-epsilon=.01
	 *                  dist-adjustment=[client lat]:[client lon]:dist_adjustment
	 * delete subdomain=x [type=y] [data=z]
	 * validate domain=x soattl=y email=z
	 * 
	 * return null if it can't parse arguments.
	 */
	public static List<RequestElement> generateRequestElements(List<String> args) {
		
		if (args.size() <= 1)
			return null;
		
		// RE data
		short opcode;
		
		RequestElement re = new RequestElement();
		
		// Populate RequestElement fields based on input string
		String op = args.get(0).toLowerCase();
		if (op.equals("add") || op.equals("a")) {
			opcode = UpdateInfo.NUP_OPCODE_ADD;
			re.opcode = opcode;
			for (String option: args) {
				if (option.equals("add") || option.equals("a")) continue;
				String[] parts = option.split("=");
				if (parts.length != 2) return null; // Invalid input

				if (parts[0].equals("subdomain")) {
					  String subdomain = parts[1];
			      if (subdomain.equals(".")) subdomain = ""; // Specal case, top level record
			      else {
					if (!Verifiers.isValidSubdomain(subdomain)) {
					  return null;
					}
				  }
			      re.subdomain = subdomain;
				}
				
				if (parts[0].equals("ttl")) {
					try {re.ttl = Integer.parseInt(parts[1]);}
					catch (NumberFormatException e) {return null;}
				}
				
				if (parts[0].equals("data")) {
					String rrtype = re.rrtype;
					if (rrtype == null) rrtype = "";
					if (Verifiers.isValidRRData(parts[1],
							rrtype)) {
						re.rrdata = parts[1];
					}
					  else {return null;}
				}
				
				if (parts[0].equals("type")) {
				  if (Verifiers.isValidRRType(parts[1])) {
						re.rrtype = parts[1];
					}
				  else {return null;}
				}
					
				if (parts[0].equals("donar-ttl")) {
					try {re.attributes.add(
							new IntegerRecordAttribute(UpdateInfo.DONAR_TTL, 
									Integer.parseInt(parts[1])));}
					catch (NumberFormatException e) {return null;}
				}
				
				if (parts[0].equals("donar-split")) {
					try {re.attributes.add(
							new DoubleRecordAttribute(UpdateInfo.SPLIT_PROPORTION, 
									Double.parseDouble(parts[1])));}
					catch (NumberFormatException e) {return null;}
				}
				
				if (parts[0].equals("dist-adjustment")) {
					try {
					  String[] sub_parts = parts[1].split(":");
					  if (sub_parts.length != 3) {return null;}
					  double lat = Double.parseDouble(sub_parts[0]);
					  double lon = Double.parseDouble(sub_parts[1]);
					  double adjustment = Double.parseDouble(sub_parts[2]);
					  DoubleListRecordAttribute toAdd = new DoubleListRecordAttribute(
					    UpdateInfo.DIST_ADJUSTMENT,	new double[] {lat, lon, adjustment} );
					  re.attributes.add(toAdd);
					}
					catch (NumberFormatException e) {return null;}
				}
				
				if (parts[0].equals("donar-epsilon")) {
					try {re.attributes.add(
							new DoubleRecordAttribute(UpdateInfo.SPLIT_EPSILON, 
									Double.parseDouble(parts[1])));}
					catch (NumberFormatException e) {return null;}
				}
			}
			if (re.checkRE() != UpdateInfo.RE_SUCCESS) {
				return null;
			}
		} else if (op.equals("delete") || op.equals("del")) {
			opcode = UpdateInfo.NUP_OPCODE_DELETE;
			re.opcode = opcode;
			
			for (String option: args) {
				if (option.equals("delete") || option.equals("del")) continue;
				String[] parts = option.split("=");
				
				if (parts.length != 2) return null; // Invalid input
				
				if (parts[0].equals("subdomain")) {
					  String subdomain = parts[1];
			      if (subdomain.equals(".")) subdomain = ""; // Specal case, top level record
			      else {
					if (!Verifiers.isValidSubdomain(subdomain)) {
					  return null;
					}
				  }
			      re.subdomain = subdomain;
				}
				
				if (parts[0].equals("data")) {
				  String rrtype = re.rrtype;
				  if (rrtype == null) rrtype = "";
				  if (Verifiers.isValidRRData(parts[1],
						rrtype)) {
				    re.rrdata = parts[1];
				  }
				  else {return null;}
				}
				
				if (parts[0].equals("type")) {
				  if (Verifiers.isValidRRType(parts[1])) {
					re.rrtype = parts[1];
				   }
				  else {return null;}
				}
				
				if (re.rrdata == null) re.rrdata = "";
				if (re.rrtype == null) re.rrtype = "";
			}
			if (re.checkRE() != UpdateInfo.RE_SUCCESS) {
				return null;
			}
		} else if (op.equals("validate")) {
			opcode = UpdateInfo.NUP_OPCODE_VALIDATE;
			re.opcode = opcode;
			re.rrtype = "";
			
			for (String option: args) {
				if (option.equals("validate")) continue;
				String[] parts = option.split("=");
				
				if (parts.length != 2) return null; // Invalid input
				
				if (parts[0].equals("domain")) {
					  String subdomain = parts[1];
			      if (subdomain.equals(".")) subdomain = ""; // Specal case, top level record
			      else {
					if (!Verifiers.isValidSubdomain(subdomain)) {
					  return null;
					}
				  }
			      re.subdomain = subdomain;
				}
				
				if (parts[0].equals("email")) {
				  String rrtype = re.rrtype;
				  if (rrtype == null) rrtype = "";
				  if (Verifiers.isValidRRData(parts[1],
						rrtype)) {
				    re.rrdata = parts[1];
				  }
				  else {return null;}
				}
				
				if (parts[0].equals("soattl")) {
					try {re.ttl = Integer.parseInt(parts[1]);}
					catch (NumberFormatException e) {return null;}
				}
			}
			if (re.checkRE() != UpdateInfo.RE_SUCCESS) {
				return null;
			}
		} else {
			System.err.println("Error: operation must be either 'add' or" +
					" 'delete' or 'validate'");
			return null;
		}
		
		LinkedList<RequestElement> list = new LinkedList<RequestElement>();
		list.add(re);
		System.out.println(re);
		return list;
	}
	
	
}
