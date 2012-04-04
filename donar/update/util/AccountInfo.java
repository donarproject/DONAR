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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import com.protomatter.syslog.Syslog;


public class AccountInfo implements Serializable {
	
	// Version of class for serialization
	private static final long serialVersionUID = 1L;
	
	// Global account info
	private String keyHash;
	private long nextSequenceNum;
	private String domainSuffix;
	private Date lastUpdateTime;
	private int soaTTL;
	private String contactEmail;
	private List<String> subdomains;
	private byte[] keyHashBytes;

	public AccountInfo(byte[] keyHash) {
		this.keyHashBytes = keyHash;
		this.keyHash = bytes2Hex(keyHash);
		this.nextSequenceNum = 0;
		this.domainSuffix = keyHash + ".donardns.net";
		this.lastUpdateTime = new Date();
		this.soaTTL = 86400;
		this.contactEmail = "admin@donardns.net";
		this.subdomains = new LinkedList<String>();
	}
	
	public AccountInfo(String keyHash) {
		this.keyHashBytes = hex2Bytes(keyHash);
		this.keyHash = keyHash;
		this.nextSequenceNum = 0;
		this.domainSuffix = keyHash + ".donardns.net";
		this.lastUpdateTime = new Date();
		this.soaTTL = 86400;
		this.contactEmail = "admin@donardns.net";
		this.subdomains = new LinkedList<String>();
	}
	
	public static String bytes2Hex(byte[] in) {
		HashMap<Byte, Character> hexMap = new HashMap<Byte, Character>();
		hexMap.put((byte) 0x00, '0');
		hexMap.put((byte) 0x01, '1');
		hexMap.put((byte) 0x02, '2');
		hexMap.put((byte) 0x03, '3');
		hexMap.put((byte) 0x04, '4');
		hexMap.put((byte) 0x05, '5');
		hexMap.put((byte) 0x06, '6');
		hexMap.put((byte) 0x07, '7');
		hexMap.put((byte) 0x08, '8');
		hexMap.put((byte) 0x09, '9');
		hexMap.put((byte) 0x0a, 'a');
		hexMap.put((byte) 0x0b, 'b');
		hexMap.put((byte) 0x0c, 'c');
		hexMap.put((byte) 0x0d, 'd');
		hexMap.put((byte) 0x0e, 'e');
		hexMap.put((byte) 0x0f, 'f');
		char[] out = new char[40];
		for (int i = 0; i < 20; i++) {
			out[2*i] = hexMap.get((byte) ((in[i] & 0xf0) >> 4));
			out[2*i + 1] = hexMap.get((byte) ((in[i] & 0x0f)));
		}
		return new String(out);
	}
	
	public static byte[] hex2Bytes(String in) {
		HashMap<Character, Byte> hexMap = new HashMap<Character, Byte>();
		hexMap.put('0', (byte) 0);
		hexMap.put('1', (byte) 1);
		hexMap.put('2', (byte) 2);
		hexMap.put('3', (byte) 3);
		hexMap.put('4', (byte) 4);
		hexMap.put('5', (byte) 5);
		hexMap.put('6', (byte) 6);
		hexMap.put('7', (byte) 7);
		hexMap.put('8', (byte) 8);
		hexMap.put('9', (byte) 9);
		hexMap.put('a', (byte) 10);
		hexMap.put('b', (byte) 11);
		hexMap.put('c', (byte) 12);
		hexMap.put('d', (byte) 13);
		hexMap.put('e', (byte) 14);
		hexMap.put('f', (byte) 15);
		
		if (in.length() != 40) {
			Syslog.info(AccountInfo.class, "Got wrong length key: " + in);
		}
		byte[] out = new byte[20];
		for (int i = 0; i < 20; i++) {
			byte b = hexMap.get(in.charAt(2*i + 1));
			b = (byte) (b | (byte) (hexMap.get(in.charAt(i*2)) << 4));
			out[i] = b;
		}

		return out;
	}
	// Getters and setters
	public String getKeyHash() {
		return keyHash;
	}
	public long getSequenceNum() {
		return nextSequenceNum;
	}
	public void incrSequenceNum() {
		this.nextSequenceNum++;
	}
	public String getDomainSuffix() {
		return domainSuffix;
	}
	public void setDomainSuffix(String newSuffix, int newTTL, String newContact) {
		// TODO change all DNS records to new suffix
		this.domainSuffix = newSuffix;
		this.soaTTL = newTTL;
		this.contactEmail = newContact;
	}
	public Date getLastUpdateTime() {
		return lastUpdateTime;
	}
	public int getSOATTL() {
		return soaTTL;
	}
	public String getContactEmail() {
		return contactEmail;
	}
	
	public List<String> getSubdomains() {
		return this.subdomains;
	}
	
	/*
	 * Adds a subdomain to the list of subdomains existing in this account.
	 */
	public void addSubdomain(String subdomain) {
		this.subdomains.add(subdomain);
	}
	
	/*
	 * Saves this account record in CRAQ.
	 */
	public void save(Socket craqSocket) throws IOException {
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
	    // Serialize
		ByteArrayOutputStream objByteStream = new ByteArrayOutputStream();
		ObjectOutputStream objOut = new ObjectOutputStream(objByteStream);
		objOut.writeObject(this);
		byte[] objBytes = objByteStream.toByteArray();
		
		// Send info to CRAQ
		String craqRequest = "SET " + this.keyHash + " " + 
			objBytes.length + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		dataOut.write(objBytes);
		dataOut.writeBytes("\r\n");
		dataOut.flush();
		
		// Read reply
		String craqReply = dataIn.readLine();
		if (craqReply.equals("STORED")) {
			return;
		} else {
			throw new IOException(
				"CRAQ reported error storing AccountInfo: " +	craqReply);
		}	
	}
	
	/*
	 * Saves this account record in CRAQ according to the XDR format.
	 */
	public void saveXDR(Socket craqSocket) throws IOException {
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(craqSocket.getOutputStream());
		
	    // Serialize
		ByteArrayOutputStream objByteStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(objByteStream);
		
		// keyHash is already hex-encoded string
		out.write(keyHashBytes);
		out.writeLong(this.nextSequenceNum);
		
		
		// Write rest of data to buffer
		XDRUtil.writeString(this.domainSuffix, out);
		
		long time = this.lastUpdateTime.getTime();

		out.writeLong(time);
		out.writeInt(this.soaTTL);
		
		XDRUtil.writeString(this.contactEmail, out);
		
		out.writeInt(this.subdomains.size());
		for (String s: this.subdomains) {
			XDRUtil.writeString(s, out);
		}
		objByteStream.flush();
		out.flush();
		// Send info to CRAQ
		
		String craqRequest = "SET " + this.keyHash + " " + 
			(objByteStream.toByteArray().length) + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
		dataOut.write(objByteStream.toByteArray());
		dataOut.writeBytes("\r\n");
		dataOut.flush();
		
		// Read reply
		String craqReply = dataIn.readLine();
		if (craqReply.length() > 5 && craqReply.subSequence(0, 6).equals("STORED")) {
			return;
		} else {
			throw new IOException(
				"CRAQ reported error storing AccountInfo: " + craqRequest + "\n" + 
				 objByteStream.toString() + "\n" + craqReply);
		}	
	}

	public static AccountInfo fromXDR(byte[] inData) throws IOException {
		ByteArrayInputStream is = new ByteArrayInputStream(inData);
		DataInputStream dis = new DataInputStream(is);
		
		byte[] keyHash = new byte[20];
		dis.readFully(keyHash);
		AccountInfo ai = new AccountInfo(keyHash);

		ai.nextSequenceNum = dis.readLong();


		
		ai.domainSuffix = XDRUtil.readString(dis);

		long time = dis.readLong();
		ai.lastUpdateTime = new Date(time);
		ai.soaTTL = dis.readInt();

		ai.contactEmail = XDRUtil.readString(dis);

		int numSubdomains = dis.readInt();

		for (int i = 0; i < numSubdomains; i++) {
			ai.subdomains.add(XDRUtil.readString(dis));
		}

		return ai;
	}
	
	public static void main (String[] args) {
		System.out.println("Testing byte conversion");
		
		String original = "6768033e2164bc477bd031a0a2d9876d79818f8f";
		byte[] test = hex2Bytes(original);
		String result = bytes2Hex(test);
		System.out.println("Result: " + result);
		System.out.println("Original: " + original);
		
		AccountInfo ai = new AccountInfo(original);
		ai.contactEmail = "pwendell@gmail.com";
		ai.subdomains.add("foo");
		Socket s = null;
		try {
			s = new Socket("localhost", 2182);
			ai.saveXDR(s);
			
			
			DataInputStream dataIn =
				new DataInputStream(s.getInputStream());
			DataOutputStream dataOut =
				new DataOutputStream(s.getOutputStream());
			
			String craqRequest = "GET " + ai.keyHash + "\r\n";
			dataOut.writeBytes(craqRequest);
			dataOut.flush();
			
			String craqReply = dataIn.readLine();
			System.out.println(craqReply);
			
			StringTokenizer replyTokens = new StringTokenizer(craqReply);
			String status = replyTokens.nextToken();
			
			if (status.equals("NOT_FOUND") || status.equals("ERROR")) {
				System.out.println("Bad reply...");
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
				
				AccountInfo ai2 = AccountInfo.fromXDR(objData);	
				
				System.out.println("Created account from serialized data: ");
				System.out.println(ai2.contactEmail);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
	}
}

