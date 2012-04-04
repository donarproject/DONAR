package donar.update.util;

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
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class CraqInterface {
	
	private Socket craqSocket;

	private Set<String> accountsInUse;
	private Lock accountsLock;
	
	public CraqInterface(Socket craqSocket) throws IOException {
		this.craqSocket = craqSocket;
		this.accountsInUse = new HashSet<String>();
		this.accountsLock = new ReentrantLock();
	}
	
	public void lockAccount(String keyHash) {
		boolean accountLocked = false;
		while (!accountLocked) {
			accountsLock.lock();
			try {
				if (accountsInUse.contains(keyHash)) {
					Thread.yield(); // Keep waiting for account to be free
				} else {
					accountsInUse.add(keyHash);
					accountLocked = true;
				}
			} finally {
				accountsLock.unlock();
			}
		}
	}
	
	public void unlockAccount(String keyHash) {
		accountsLock.lock();
		try {
			accountsInUse.remove(keyHash);
		} finally {
			accountsLock.unlock();
		}
	}
	
	@SuppressWarnings("deprecation")
	public AccountInfo getAccountInfo(String keyHash) throws IOException {
		
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
		// Request AccountInfo from CRAQ
		String craqRequest = "GET " + keyHash + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		
		// Check response status
		String craqReply = dataIn.readLine();
		//System.err.println("\tDEBUG: getAccountInfo: craqReply is: " + craqReply);
		StringTokenizer replyTokens = new StringTokenizer(craqReply);
		if (!replyTokens.hasMoreTokens()) {
			throw new IOException("Blank reply from CRAQ reading in AccountInfo");
		}
		String status = replyTokens.nextToken();
		
		if (status.equals("NOT_FOUND")) {
			return null; // No such account
		}
		
		else if (status.equals("VALUE")) {
			
			// Read in AccountInfo
			int dataLen = Integer.parseInt(replyTokens.nextToken());
			byte[] objData = new byte[dataLen];
			try {
				dataIn.readFully(objData);
			} catch (SocketTimeoutException e) {
				throw new IOException("CRAQ timed out reading in AccountInfo");
			}
			dataIn.readLine(); // Skip past terminating \r\n
			
			// Convert to object and return
			ByteArrayInputStream bytesIn = new ByteArrayInputStream(objData);
			ObjectInputStream objIn = new ObjectInputStream(bytesIn);
			try {
				return (AccountInfo) objIn.readObject();
			} catch (Exception e) {
				throw new IOException(e.getMessage());
			}		
			
		} else {
			throw new IOException("CRAQ reportedd error reading in AccountInfo: "
					+ craqReply);
		}
		
	}
	
	public String getAccount(String domain) throws IOException {
		
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
        // Request AccountInfo from CRAQ
		String craqRequest = "GET " + domain + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		
		// Check response status
		String craqReply = dataIn.readLine();
		//System.err.println("\tDEBUG: getAccountInfo: craqReply is: " + craqReply);
		StringTokenizer replyTokens = new StringTokenizer(craqReply);
		if (!replyTokens.hasMoreTokens()) {
			throw new IOException("Blank reply from CRAQ reading in account mapping");
		}
		String status = replyTokens.nextToken();
		//System.err.println("\tDEBUG: getAccountInfo: status is: " + status);
		
		if (status.equals("NOT_FOUND")) {
			// Account not found returns null value
			return null;		
		} else if (status.equals("VALUE")) {			
			// Read in keyHash
			String keyHash = dataIn.readLine(); // Skip past terminating \r\n
			if (keyHash.length() == 0)
				return null;
			else
				return keyHash;
		} else {
			throw new IOException("CRAQ reported error reading in account mapping");
		}
		
	}
	
	public void setAccountInfo(AccountInfo ai) throws IOException {
		
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
	    // Serialize
		ByteArrayOutputStream objByteStream = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(objByteStream);
		objOut.writeObject(ai);
		byte[] objBytes = objByteStream.toByteArray();
		
		// Send info to CRAQ
		String craqRequest = "SET " + ai.getKeyHash() + " " + 
			objBytes.length + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		dataOut.write(objBytes);
		dataOut.writeBytes("\r\n");
		dataOut.flush();
		
		// Read reply
		String craqReply = dataIn.readLine();
		if (craqReply.contains("STORED")) {
			return;
		} else {
			throw new IOException("CRAQ reported error storing AccountInfo: " +
					craqReply);
		}
		
	}
	
	public void setAccount(String domain, String keyHash) throws IOException {
	
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
		// Send info to CRAQ
		String craqRequest = "SET " + domain + " " + 
			keyHash.length() + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		dataOut.writeBytes(keyHash);
		dataOut.writeBytes("\r\n");
		dataOut.flush();
		
		// Read reply
		String craqReply = dataIn.readLine();
		if (craqReply.contains("STORED")) {
			return;
		} else {
			throw new IOException("CRAQ reported error storing account mapping");
		}
		
	}

public void removeAccount(String domain) throws IOException {
	
	setAccount(domain, "");
	
}

}
