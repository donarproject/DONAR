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

import java.util.*;
import java.net.*;

import donar.update.UpdateInfo;

public class Verifiers {
	
	public static boolean isValidOpcode(short opcode)
	{
		return opcode == UpdateInfo.NUP_OPCODE_ADD ||
		       opcode == UpdateInfo.NUP_OPCODE_DELETE ||
		       opcode == UpdateInfo.NUP_OPCODE_VALIDATE;
	}
	
	public static boolean isValidSubdomain(String subdomain)
	{
		// Whole thing too long?
		if (subdomain.length() > UpdateInfo.NUP_MAX_SUBDOMAIN_LEN)
			return false;
		
		// Check individual parts of subdomain
		StringTokenizer tokens = new StringTokenizer(subdomain, ".");
		while (tokens.hasMoreTokens()) {
			String part = tokens.nextToken();
			// Part too long or too short?
			if (part.length() > UpdateInfo.NUP_MAX_SUBDOMAIN_PART_LEN ||
				part.length() < UpdateInfo.NUP_MIN_SUBDOMAIN_PART_LEN)
					return false;
			// Part has invalid character?
			char[] chars = part.toCharArray();
			if (!Character.isLetterOrDigit(chars[0]) ||
				!Character.isLetterOrDigit(chars[chars.length - 1]))
				// First and last character cannot be hyphen but others can
				return false;
			for (int i = 1; i < chars.length - 1; i++) {
				if (!Character.isLetterOrDigit(chars[i]) && !(chars[i] == '-'))
					return false;
			}
		}
		
		return true;		
	}
	
	public static boolean isValidRRType(String rrtype)
	{
		return rrtype.equals("A") ||
			   rrtype.equals("CNAME") ||
			   rrtype.equals("MX") ||
			   rrtype.equals("A+") ||
			   rrtype.equals("TXT") ||
			   rrtype.equals("HTTP");
	}
	
	public static boolean isValidRRData(
			String rrdata, String rrtype)
	{	
		if (rrtype.equals("A") || rrtype.equals("A+")) {
			// Make sure only numbers and dots are present
			for (char c : rrdata.toCharArray()) {
				if (!Character.isDigit(c) &&
						!(c == '.'))
					return false;
			}
			// Make sure IPv4 address is in valid range/format
			InetAddress addr;
			try {
				addr =
					InetAddress.getByName(rrdata);
			} catch (Exception e) {
				return false;
			}
			return addr != null;
			
		} else if (rrtype.equals("AAAA")) {
			// Make sure there is at least one colon
			if (rrdata.indexOf(":") < 0)
				return false;
			// Make sure IPv6 address is in valid format
			InetAddress addr;
			try {
				addr =
					InetAddress.getByName(rrdata);
			} catch (Exception e) {
				return false;
			}
			return addr != null;
			
		} else if (rrtype.equals("CNAME")) {
			return Verifiers.isValidSubdomain(rrdata);
			
		} else if (rrtype.equals("MX")) {
			String[] parts = rrdata.split("\\s");
			// Make sure priority and domain are present
			if (parts.length != 2)
				return false;
			// Make sure priority is integer
			try {
				Integer.parseInt(parts[0]);
			} catch (Exception e) {
				return false;
			}
			// Make sure domain is valid
			return Verifiers.isValidSubdomain(parts[1]);
			
		} else if (rrtype.equals("NS")) {
			return Verifiers.isValidSubdomain(rrdata);
			
		} else if (rrtype.equals("TXT") || rrtype.equals("") || rrtype.equals("HTTP")) {
			return true; // TXT or blank type always valid
			
		} else {		
			return false;
		}
		
	}

}
