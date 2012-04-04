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

import java.util.LinkedList;
import java.util.List;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import donar.dns.attrs.RecordAttribute;

public class DNSRecord implements Serializable, Comparable<DNSRecord>  {
	/*
	 * Stores several DNS records for a given subdomain of an account.
	 * For example: might store MX, AA, and AAAA records for 
	 * www.cs.princeton.edu. This data structure is designed to make
	 * queries very fast, requiring only one craq lookup (with the fully
	 * qualified domain name as the key) and automatically producing a list
	 * of responses.
	 */
	private static final long serialVersionUID = 3L;
	public String type;
	public String content;
	public int id;
	public int ttl;
	public List<RecordAttribute> attributes;
	public InetAddress ip; // For geolocation
	public double proportion; // For load balaning
	
	public DNSRecord(){
		this.attributes = new LinkedList<RecordAttribute>();
	}
	
	public DNSRecord(String type, String content, int ttl, List<RecordAttribute> attributes) {
		this.type = type;
		this.content = content;
		this.ttl = ttl;
		this.attributes = attributes;
		if (type.equals("A")) {
			try {
				this.ip = InetAddress.getByName(this.content);
			}
			catch (UnknownHostException e) {
				throw new NumberFormatException("Bad IP address");
			}
		}
		
	}

	public int compareTo(DNSRecord other) {
		if (this.id < other.id) return -1;
		if (this.id > other.id) return 1;
		return 0;
	}
}
