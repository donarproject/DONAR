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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import com.protomatter.syslog.Syslog;

import donar.dns.attrs.DoubleRecordAttribute;
import donar.dns.attrs.IntegerRecordAttribute;
import donar.dns.attrs.RecordAttribute;
import donar.dns.attrs.ShortRecordAttribute;

public class SubdomainInfo implements Serializable{
	
	// Version of class for serialization
	private static final long serialVersionUID = 2L;
	
	private List<DNSRecord> records;
	private String fqdn;         // Fully qualified domain name
	private String accountHash;  // Account this record belongs to
	private long sequenceNum;
	
	public SubdomainInfo(String fqdn, String accountHash) {
		this.records = new ArrayList<DNSRecord>();
		this.fqdn = fqdn;
		this.accountHash = accountHash;
	}
	
	public void replaceSuffix(String newSuffix) throws IOException {
		// Make sure existing fqdn ends with "donardns.net"
		int test = fqdn.lastIndexOf("donardns.net");
		if (!(test == (fqdn.length() - "donardns.net".length()))) {
			Syslog.error(this, test);
			Syslog.error(this, fqdn.trim().length());
			Syslog.error(this, "Attemtped to validate already validated subdomain");
			Syslog.error(this, "Account suffix: " + fqdn);
			throw new IOException("Attempted to validate already validated" +
					"account");
		}
		fqdn = fqdn.replaceAll("\\w+.donardns.net$", newSuffix);
		// TODO, delete old suffix
		Syslog.debug(this, "Changed suffix: ");
		Syslog.debug(this, "New fqdn: " + fqdn);
	}
	
	public String getFqdn() {
		return this.fqdn;
	}
	
	public void incrementSequenceNumber() {
		this.sequenceNum++;
	}
	
	public String getAccountHash() {
		return this.accountHash;
	}
	
	public List<DNSRecord> getRecords() {
		return this.records;
	}
	
	public void addRecord(String type, String content, int ttl, List<RecordAttribute> attributes) {
		DNSRecord newRecord = new DNSRecord(type, content, ttl, attributes);
		// If record with this type and content exists, just update TTL and attributes
		for (DNSRecord r: this.records) {
			// System.out.println("Existing record: " + r.type + " " + r.content);
			if (r.type.equals(type) && r.content.equals(content)) {
				r.ttl = ttl;
				r.attributes = attributes;
				return;
			}
		}
		this.incrementSequenceNumber();
		this.records.add(newRecord);
	}
	
	public void delRecord(String type, String content) {
		Syslog.debug(this, "Record deletion requested for subdomain " + this.getFqdn());
		Syslog.debug(this, "type: " + type + " content: " + content);
		if (records != null) {
			ListIterator<DNSRecord> recordIter = records.listIterator();
			while(recordIter.hasNext()) {
				DNSRecord nextRecord = recordIter.next();
				if (type.equals("") && content.equals("")) {
					Syslog.debug(this, "Removing record: " + nextRecord);
					recordIter.remove();
				}
				
				else if (type.equals("") &&
						nextRecord.content.equals(content)) {
					Syslog.debug(this, "Removing record: " + nextRecord);
					recordIter.remove();
				}
				
				else if (content.equals("") &&
						nextRecord.type.equals(type)) {
					Syslog.debug(this, "Removing record: " + nextRecord);
					recordIter.remove();
				}
				
				else if (nextRecord.type.equals(type) &&
						 nextRecord.content.equals(content)) {
					Syslog.debug(this, "Removing record: " + nextRecord);
					recordIter.remove();
				}
			}
		}
	}
	
	/*
	 * Saves this subdomain record in CRAQ.
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
		String craqRequest = "SET " + this.fqdn + " " + 
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
				"CRAQ reported error storing SubdomainInfo: " +	craqReply);
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
		
		XDRUtil.writeString(this.fqdn, out);
		out.writeLong(sequenceNum);
		
		// Records
		out.writeInt(this.records.size());
		for (DNSRecord r: records) {
			XDRUtil.writeString(r.type, out);
			XDRUtil.writeString(r.content, out);
			out.writeInt(r.ttl);
			out.writeInt(r.attributes.size());
			for (RecordAttribute a: r.attributes) {
				a.writeXDR(out);
			}
		}
		out.write(AccountInfo.hex2Bytes(this.accountHash));
		
		objByteStream.flush();
		out.flush();
		// Send info to CRAQ
		
		String craqRequest = "SET " + fqdn.trim() + " " + 
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
				"CRAQ reported error storing SubdomainInfo: " + craqRequest + "\n" + 
				 objByteStream.toString() + "\n" + craqReply);
		}	
	}
	
	public static SubdomainInfo fromXDR(byte[] inData) throws IOException {
		ByteArrayInputStream is = new ByteArrayInputStream(inData);
		DataInputStream dis = new DataInputStream(is);
		
		String fqdn = XDRUtil.readString(dis);
		long sequenceNumber = dis.readLong();
		int numRecords = dis.readInt();
		LinkedList<DNSRecord> recs = new LinkedList<DNSRecord>();
		for (int i = 0; i < numRecords; i++) {
			String type = XDRUtil.readString(dis);
			String content = XDRUtil.readString(dis);
			int ttl = dis.readInt();
			int numAttrs = dis.readInt();
			LinkedList<RecordAttribute> attrs = new LinkedList<RecordAttribute>();
			for (int j = 0; j < numAttrs; j++) {
				int attType = dis.readInt();
				int attLength = dis.readInt();
				byte[] attData = new byte[attLength];
				dis.readFully(attData);
				RecordAttribute newAtt = null;
				switch (attLength) {
					case 2: newAtt = new ShortRecordAttribute(); break;
					case 4: newAtt = new IntegerRecordAttribute(); break;
					case 8: newAtt = new DoubleRecordAttribute(); break;
					default: continue;
				}

				newAtt.typeID = (short) attType;
				newAtt.setData(attData);
				attrs.add(newAtt);
			}
			recs.add(new DNSRecord(type, content, ttl, attrs));
		}
		byte[] accountHash = new byte[20];
		dis.readFully(accountHash);
		String accountHashStr = AccountInfo.bytes2Hex(accountHash);
		SubdomainInfo out = new SubdomainInfo(fqdn, accountHashStr);
		out.records = recs;
		out.sequenceNum = sequenceNumber;
		return out;
	}
	
	public static void main (String[] args) {
		SubdomainInfo ai = new SubdomainInfo("patrick.wendell.com", 
				"6768033e216468247bd031a0a2d9876d79818f8f");
		Socket s = null;
		try {
			s = new Socket("localhost", 2182);
			ai.saveXDR(s);
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
	}

}
