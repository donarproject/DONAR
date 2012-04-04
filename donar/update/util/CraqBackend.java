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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.protomatter.syslog.Syslog;

import donar.dns.attrs.RecordAttribute;

public class CraqBackend implements DONARBackend {
	private Socket craqSocket;
	private AccountInfo currentAccount;
	private Set<String> accountsInUse;
	private Lock accountsLock;	
	
	/*
	 * CRAQ-based backend for DONAR. Stores DONAR data in a CRAQ
	 * distributed storage layer. Assumes there is a CRAQ client running on
	 * host CRAQ_HOST at CRAQ_PORT. 
	 */
	public CraqBackend(String craqHost, int craqPort) throws IOException {
		this.craqSocket = new Socket(craqHost, craqPort);
		this.accountsInUse = new HashSet<String>();
		this.accountsLock = new ReentrantLock();
	}

	
	/*
	 * Returns a PowerDNS-complaint query answer. For backends which require
	 * powerdns pipe-backend (i.e. not mysql!). This implementation uses
	 * geolocation to try and return the closes answer to the client first.
	 */
	public List<DNSRecord> answerQuery(String qname, String qclass, 
			String qtype, String id, String remoteIPAddress) 
			throws IOException {
		List<DNSRecord> recordList;
		
		// We only store Internet DNS records
		if (!qclass.equals("IN")) {
			return new ArrayList<DNSRecord>();
		}
		
		// Look up all records for given qname
		SubdomainInfo subdomain = getSubdomain(qname);
		if (subdomain == null) {
			return new ArrayList<DNSRecord>();
		}
		recordList = subdomain.getRecords();
		
		// Add SOA record if necessary
		if (qtype.equals ("SOA")) {
			if (recordList == null)
				recordList = new LinkedList<DNSRecord>();
			SimpleDateFormat soaSerialFormat = new SimpleDateFormat("yymmddHHmm");
			DNSRecord soaRecord = new DNSRecord();
			soaRecord.ttl = currentAccount.getSOATTL(); 
			soaRecord.type = "SOA";
			soaRecord.content = "ns1.donardns.net " + // primary nameserver 
													  // for domain
			  currentAccount.getContactEmail() + " " + 
			  soaSerialFormat.format(currentAccount.getLastUpdateTime()) +
			  " " +         // serial
			  "10800 "  +   // refresh
			  "3600 "   +   // retry
			  "604800 " +   // expire
			  "3600";       // default_ttl
			recordList.add(0, soaRecord);
		}
		
		// Filter out records that don't belong
		if (! qtype.equals("ANY")) { // No filtering for ANY query
			ListIterator<DNSRecord> iter =
				recordList.listIterator();
			while (iter.hasNext()) {
				if (!iter.next().type.equals(qtype)) {
					iter.remove();
				}
			}
		}

		Syslog.debug(this, "\tReturning records: " + recordList);	
		
		return recordList;
	}

	/* 
	 * Check if an account described by keyHash exists, and if not generate
	 * a new account with that hash.
	 */
	public void assureKey(String keyHash) throws IOException {
		Syslog.debug(this, "assureKey: locking account " + keyHash);
		bindAccount(keyHash);
		if (currentAccount == null) {
			currentAccount = new AccountInfo(keyHash);
			currentAccount.saveXDR(craqSocket);
			Syslog.debug(this, "assureKey: new account created: " +
				keyHash);
		}
		unbindAccount(keyHash);
		Syslog.debug(this, "assureKey: account unlocked");
	}

	/*
	 * Gets the current sequence number for account with key keyHash.
	 */
	public long getSequenceNum(String keyHash) throws IOException {
		AccountInfo ai = getAccountInfo(keyHash);
		long sequenceNum = ai.getSequenceNum();
		return sequenceNum;
	}

	public void unbindAccount(String keyHash) throws IOException {
		if (this.currentAccount != null) {
			this.currentAccount.saveXDR(craqSocket);
			Syslog.debug(this, "Saved: " + this.currentAccount.getKeyHash());
		}
		else {
			Syslog.error(this, "Backend unbind called but no current account");
		}
		this.currentAccount = null;
	}
	/* 
	 * Locks an account so account data cannot be overwritten while procesing
	 * or modifying this account's data.
	 */
	public void bindAccount(String keyHash) throws IOException {
		
		/*boolean accountLocked = false;
		while (accountsInUse.contains(keyHash)) 
			Thread.yield(); // Keep waiting for account to be free

		accountsInUse.add(keyHash);
		accountLocked = true;
		*/
		try {
		  currentAccount = getAccountInfo(keyHash);
		  if (currentAccount != null) {
			Syslog.debug(this, "Bound " + currentAccount.getKeyHash());
		  }
		  else {
			Syslog.error(this, "Bind account failed for hash: " + keyHash);
		  }
		}
		catch (IOException e) {
			currentAccount = null;
			Syslog.error(this, "Bind account failed for hash: " + keyHash);
		}

	}
	
	/* 
	 * Deletes a DNS records held by an account for a given subdomain, with
	 * a given type and content. If type or content are blank strings, then
	 * they are ignored in the search. If subdomain is blank, deletes top
	 * level records.
	 * */
	public void delRecords(String subdomain, String type,
	  String content) throws IOException {
		if (this.currentAccount == null) {
			throw new IOException("Tried to use backend with unbound account");
		}
		// Assure there is a subdomain already
		String fqdn;
		if (subdomain.equals("")) {
			fqdn = currentAccount.getDomainSuffix();
		}
		else {
			fqdn = subdomain + "." + currentAccount.getDomainSuffix();
		}
		SubdomainInfo newSD = getSubdomain(fqdn);
		if (newSD == null) {
			return;
		}
		newSD.delRecord(type, content);
		newSD.incrementSequenceNumber();
		newSD.saveXDR(this.craqSocket);
	}
	
	/* 
	 * Adds a DNS record to account described by keyHash. If the subdomain
	 * string is blank, adds a record for the suffex itself. If record exists with same
	 * subdomain, type and content. Replaces data (ttl/atributes).
	 * */
	public void addRecord(String subdomain, String type,
	  String content, int ttl, List<RecordAttribute> attributes) throws IOException {
		Syslog.debug(this, "Adding record for " + subdomain);
		if (this.currentAccount == null) {
			throw new IOException("Tried to use backend with unbound account");
		}
		String fqdn;
		// See if there is a record already
		if (subdomain.equals("")) {
			fqdn = currentAccount.getDomainSuffix();
		}
		else {
			fqdn = subdomain + "." + currentAccount.getDomainSuffix();
		}	
		SubdomainInfo newSD = getSubdomain(fqdn);
		if (newSD == null) {
			Syslog.debug(this, "No subdomain record found for [" + fqdn + "] " +
					"creating new subdomain record.");
			newSD = new SubdomainInfo(fqdn, currentAccount.getKeyHash());
			currentAccount.addSubdomain(subdomain);
		}
		newSD.addRecord(type, content, ttl, attributes);
		newSD.saveXDR(this.craqSocket);
		Syslog.debug(this, "Added record succesfully...");
	}
	
	/*
	 * Gets a sobdomain record from CRAQ if one exists. Otherwise returns
	 * null.
	 */
	private SubdomainInfo getSubdomain(String fqdn) throws IOException {
		// Get socket
		DataInputStream dataIn =
			new DataInputStream(this.craqSocket.getInputStream());
		DataOutputStream dataOut =
			new DataOutputStream(this.craqSocket.getOutputStream());
		
		// Request SubdomainInfo from CRAQ
		String craqRequest = "GET " + fqdn + "\r\n";
		dataOut.writeBytes(craqRequest);
		dataOut.flush();
	
		// Check response status
		String craqReply = dataIn.readLine();

		Syslog.debug(this, "Sending request to craq: " + craqRequest); 
		Syslog.debug(this, "Reply from CRAQ: " + craqReply);
		StringTokenizer replyTokens = new StringTokenizer(craqReply);
		if (!replyTokens.hasMoreTokens()) {
			throw new IOException("Blank reply from CRAQ " +
				"reading in SubdomainInfo");
		}
		String status = replyTokens.nextToken();
		
		if (status.contains("NOT_FOUND")) {
			return null;
		}
		
		else if (status.equals("VALUE")) {
			int dataLen = Integer.parseInt(replyTokens.nextToken());
			if (dataLen == 0) {
				dataIn.readLine();
				return null;
			}
			byte[] objData = new byte[dataLen];
			try {
				dataIn.readFully(objData);
			} catch (SocketTimeoutException e) {
				Syslog.error(this, "Error reading reply from CRAQ");
				throw new IOException(
					"CRAQ timed out reading in SubdomainInfo");
			}
			dataIn.readLine(); // Skip past terminating \r\n
			
			try {
				return SubdomainInfo.fromXDR(objData);
			} catch (Exception e) {
				Syslog.error(this, "Problem unmarshalling subdomain");
				throw new IOException(e.getMessage());
			}		
			
		} else {
			throw new IOException(
				"CRAQ reported error reading in SubdomainInfo: " + craqReply);
		}
	}
	
	
	/* 
	 * Updates the suffix of an account. Must go through all subdomains
	 * and update the suffix.
	 */
	public void updateSuffix(String newSuffix) throws IOException {
		if (this.currentAccount == null) {
			Syslog.error(this, "Tried to use backend with unbound account");
			throw new IOException("Tried to use backend with unbound account");
		}
		List<String> subdomains = currentAccount.getSubdomains();
		Syslog.debug(this, "Updating " + subdomains.size() + " subdomains");
		for (String subdomain: subdomains) {
			// Update each subdomain in data store
			String fqdn;
			Syslog.debug(this, "here0");
			if (subdomain == "") {
				fqdn = currentAccount.getDomainSuffix();
			}
			else {
				fqdn = subdomain + "." +
				currentAccount.getDomainSuffix();
			}
			SubdomainInfo sd = null;
			Syslog.debug(this, "here1");
			try {
				sd = getSubdomain(fqdn);
			}
			catch (Exception e) {
				Syslog.error(this, "Error getting subdomain " + e.getMessage());
			}
			Syslog.debug(this, "here2");
			sd.replaceSuffix(newSuffix);
			Syslog.debug(this, "Saving new subdomain: " + sd.getFqdn());
			sd.saveXDR(craqSocket);
		}
		
		currentAccount.setDomainSuffix(newSuffix, 1000, "admin@namecast.org");
	}
	
	/*
	 * Gets an AccountInfo object from the data store, given the keyHash
	 * for that account.
	 */
	private AccountInfo getAccountInfo(String keyHash) throws IOException {
		
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
		Syslog.debug(this, "getAccountInfo: craqReply is: " + craqReply);
		StringTokenizer replyTokens = new StringTokenizer(craqReply);
		if (!replyTokens.hasMoreTokens()) {
			throw new IOException("Blank reply from CRAQ reading in AccountInfo");
		}
		String status = replyTokens.nextToken();
		
		if (status.equals("NOT_FOUND") || status.equals("ERROR")) {
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
			
			Syslog.debug(this, "De-cerealizing account: " + keyHash);
			return AccountInfo.fromXDR(objData);	

			
		} else {
			throw new IOException("CRAQ reported error reading in AccountInfo: "
					+ keyHash + " CRAQ said: " + craqReply);
		}
	}
	
	/*
	 * Increment sequence number
	 */
	public void incrementSequenceNum() throws IOException{
		if (this.currentAccount == null) {
			throw new IOException("Tried to use backend with unbound account");
		}
		currentAccount.incrSequenceNum();
	}
	
	/* 
	 * Remove account locks.
	 */
	public void ount(String keyHash) throws IOException {
		accountsLock.lock();
		try {
			accountsInUse.remove(keyHash);
			currentAccount.saveXDR(craqSocket);
			currentAccount = null;
		} finally {
			accountsLock.unlock();
		}
	}


	@Override
	public String getSuffix() {
		return this.currentAccount.getDomainSuffix();
	}

}
