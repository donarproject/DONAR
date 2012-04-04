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

import java.io.IOException;
import java.util.List;

import donar.dns.attrs.RecordAttribute;

/*
 * Interface for DONAR backends. This interface is designed to allow
 * flexible backends for DONAR. It serves both the DONAR Update Server
 * (NUPserver) and the PowerDNS pipe-backend.
 */

public interface DONARBackend {
	
	/* 
	 * Check if an account described by keyHash exists, and if not generate
	 * a new account with that hash.
	 */
	void assureKey(String keyHash) throws IOException;

	/*
	 * Gets the current sequence number for account with key keyHash.
	 */
	long getSequenceNum(String keyHash) throws IOException;

	
	/*
	 * Binds this backend to a specific account in order to perform 
	 * operations on that account.
	 */
	void bindAccount(String keyHash) throws IOException ;
	
	/* 
	 * Increments the sequence number of an account
	 */
	void incrementSequenceNum() throws IOException;
	
	/*
	 * Unbinds account.
	 */
	void unbindAccount(String keyHash) throws IOException ;
	
	/* 
	 * Adds a DNS record to account described by keyHash. 
	 * */
	public void addRecord(String subdomain, String type,
	  String content, int ttl, List<RecordAttribute> attributes) throws IOException;
	

	/* 
	 * Deletes a DNS records held by an account for a given subdomain, with
	 * a given type and content. If type or content are blank strings, then
	 * they are ignored in the search.
	 * */
	public void delRecords(String subdomain, String type,
	  String content) throws IOException;
	
	/* 
	 * Updates the suffix of an account. Must go through all subdomains
	 * and update the suffix.
	 */
	public void updateSuffix(String newSuffix) throws IOException;
		
	/**
	 * Get the suffix currently held by this account.
	 */
	public String getSuffix();
	
	/*
	 * Returns a PowerDNS-complaint query answer. For backends which require
	 * powerdns pipe-backend (i.e. not mysql!).
	 */
	List<DNSRecord> answerQuery(String qname, String qclass, String qtype,
			String id, String remoteIPAddress) throws IOException; 
	
}