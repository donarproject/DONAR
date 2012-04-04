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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import donar.dns.attrs.RecordAttribute;
import donar.update.UpdateInfo;

public class RequestElement {	
	// Class to hold data for one request element
	public RequestElement(short opcode, String subdomain,
			  String rrtype, String rrData,
			  int ttl) {
		this.opcode = opcode;
		this.subdomain = subdomain;
		this.rrtype = rrtype;
		this.rrdata = rrData;
		this.ttl = ttl;
		this.attributes = new LinkedList<RecordAttribute>();
	}
	
	public RequestElement(short opcode, String subdomain,
						  String rrtype, byte[] rrDataBytes,
						  int ttl) {
		this.opcode = opcode;
		this.subdomain = subdomain;
		this.rrtype = rrtype;
		this.rrDataBytes = rrDataBytes;
		this.ttl = ttl;
		this.attributes = new LinkedList<RecordAttribute>();
		this.rrdata = new String(rrDataBytes);
	}
	
	public RequestElement() {
		this.attributes = new LinkedList<RecordAttribute>();
	}
	
	public short opcode;
	public String subdomain;
	public String rrtype;
	public String rrdata;
	public byte[] rrDataBytes;
	public int ttl;
	public List<RecordAttribute> attributes;
	
	
	/* 
	 * Writes data for this record to an output stream, according to 
	 * NUP specification.
	 */
	public void writeToStream(DataOutputStream dataOut) throws IOException {
		dataOut.writeShort(opcode);
		dataOut.writeShort(subdomain.length());
		dataOut.writeBytes(subdomain);
		dataOut.writeShort(rrtype.length());
		dataOut.writeBytes(rrtype);
		
	
		dataOut.writeInt(rrdata.length());
		dataOut.writeBytes(rrdata);
		dataOut.writeInt(ttl);

		if (this.attributes == null) {
			dataOut.writeShort(0);
		}
		else {
			dataOut.writeShort(this.attributes.size());
			for (RecordAttribute a : this.attributes) {
				a.writeRecord(dataOut);
			}
		}

	}
	
	public short checkRE()
	{
		// Check opcode
		if (!Verifiers.isValidOpcode(opcode)) {
			return UpdateInfo.RE_INVALID_OPCODE;
		}
		
		// Check other fields based on opcode
		if (opcode == UpdateInfo.NUP_OPCODE_ADD) {
			if (subdomain == null || !Verifiers.isValidSubdomain(subdomain))
				return UpdateInfo.RE_INVALID_SUBDOMAIN;
			if (rrtype == null || !Verifiers.isValidRRType(rrtype))
				return UpdateInfo.RE_INVALID_RR_TYPE_UNSUPPORTED;
			if (rrdata == null || !Verifiers.isValidRRData(rrdata, rrtype))
				return UpdateInfo.RE_INVALID_RR_DATA;
			if (ttl <= 0)
				return UpdateInfo.RE_INVALID_TTL;
		} else if (opcode == UpdateInfo.NUP_OPCODE_DELETE) {
			if (subdomain == null || !Verifiers.isValidSubdomain(subdomain))
				return UpdateInfo.RE_INVALID_SUBDOMAIN;
		} else if (opcode == UpdateInfo.NUP_OPCODE_VALIDATE) {
			if (subdomain == null || !Verifiers.isValidSubdomain(subdomain))
				return UpdateInfo.RE_INVALID_SUBDOMAIN;
			if (ttl <= 0)
				return UpdateInfo.RE_INVALID_TTL;
		}
		
		// All is good if it makes it this far
		return UpdateInfo.RE_SUCCESS;
	}
	
	public String toString() {
		String out =  "Request Element:\nopcode\t" + this.opcode + 
			"\nsubdomain\t" + this.subdomain + "\nrrtype\t" + this.rrtype +
			"\nrrdata\t" + this.rrdata + "\nttl\t" + this.ttl + "\n";
		if (this.attributes != null) {
			for (RecordAttribute att: this.attributes) {
				out += att.toString() + "\n";
			}
		}
		return out + "\n";
	}
}
