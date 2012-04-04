package donar.update;

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

public class UpdateInfo {
	
	// NUP constants
	public static final int SHA1_HASH_BYTES = 160 / 8;
	public static final byte[] NUP_MAGIC_STRING =
		{68, 79, 78, 65, 82}; // "DONAR" 
	public static final short NUP_VERSION = 0;
	public static final short NUP_OPCODE_ADD = 0;
	public static final short NUP_OPCODE_DELETE = 1;
	public static final short NUP_OPCODE_VALIDATE = 2;
	public static final int NUP_MAX_SUBDOMAIN_LEN = 255 - 128;
	public static final int NUP_MAX_SUBDOMAIN_PART_LEN = 63;
	public static final int NUP_MIN_SUBDOMAIN_PART_LEN = 1;
	public static final int NUP_MAX_PACKET_LENGTH = 1500;
	public static final int NUP_DEFAULT_SERVER_PORT = 21001;
	
	// Enumaration for attribute types
	public static final short DONAR_TTL = 1;
	public static final short ATTRIBUTE_LATITUDE = 2;
	public static final short ATTRIBUTE_LONGITUDE = 3;
	public static final short SPLIT_PROPORTION = 4;
	public static final short SPLIT_EPSILON = 5;
	public static final short BANDWIDTH_CAP = 6;
	public static final short EXPIRATION_TIME = 7;
	public static final short BIDIR_STATIC_MAP = 8;
	public static final short ONEWAY_STATIC_MAP = 9;
	public static final short DIST_ADJUSTMENT = 10;
	
	// Status map
	private static Map<Short, String> packetCodeDescriptionMap;
	private static Map<Short, String> reCodeDescriptionMap;
	
	// Status constants
	public static final short PACKET_SUCCESS = 0;
	public static final short PACKET_FAILURE = 100;
	public static final short PACKET_PARTIAL_SUCCESS = 101;
	public static final short PACKET_FAILURE_INVALID_RE = 102;
	public static final short PACKET_FAILURE_NO_RE = 103;
	public static final short PACKET_INVALID_SIGNATURE = 200;
	public static final short PACKET_INVALID_SEQNUM = 300;
	public static final short PACKET_NUP_VERSION_UNSUPPORTED = 400;
	public static final short PACKET_NUP_VERSION_OBSOLETE = 401;
	public static final short PACKET_MALFORMED = 500;
	public static final short PACKET_MALFORMED_TOO_SHORT = 501;
	public static final short PACKET_MALFORMED_WRONG_REQ_ELEM_NUMBER = 502;
	
	public static final short RE_SUCCESS = 0;
	public static final short RE_INVALID_OPCODE = 100;
	public static final short RE_INVALID_SUBDOMAIN = 200;
	public static final short RE_INVALID_SUBDOMAIN_TOO_LONG = 201;
	public static final short RE_INVALID_SUBDOMAIN_INVALID_CHARS = 202;
	public static final short RE_INVALID_RR_TYPE = 300;
	public static final short RE_INVALID_RR_TYPE_UNSUPPORTED = 301;
	public static final short RE_INVALID_RR_DATA = 400;
	public static final short RE_INVALID_RR_DATA_COULD_NOT_FIND = 401;
	public static final short RE_INVALID_TTL = 500;
	public static final short RE_OTHER_ERROR = 600;
	public static final short RE_OTHER_ERROR_PACKET= 601;
	public static final short RE_OTHER_ERROR_RE = 602;	
	
	private static void buildCodeDescriptionMaps() {
		
		packetCodeDescriptionMap = new HashMap<Short, String>();
		reCodeDescriptionMap = new HashMap<Short, String>();
		
		// Packet error codes
		packetCodeDescriptionMap.put(PACKET_SUCCESS,
				"Update request successfully processed");
		packetCodeDescriptionMap.put(PACKET_FAILURE,
				"Error: update request failed");
		packetCodeDescriptionMap.put(PACKET_PARTIAL_SUCCESS,
				"Error: update request partially successful");
		packetCodeDescriptionMap.put(PACKET_FAILURE_INVALID_RE,
				"Error: update request failed due to invalid RE");
		packetCodeDescriptionMap.put(PACKET_FAILURE_NO_RE,
				"Error: update request contained no REs");
		packetCodeDescriptionMap.put(PACKET_INVALID_SIGNATURE,
				"Error: update request contained invalid signature");
		packetCodeDescriptionMap.put(PACKET_INVALID_SEQNUM,
				"Error: update request contained invalid sequence number");
		packetCodeDescriptionMap.put(PACKET_NUP_VERSION_UNSUPPORTED,
				"Error: update request packet version unsupported");
		packetCodeDescriptionMap.put(PACKET_NUP_VERSION_OBSOLETE,
				"Error: update request packet version obsolete");
		packetCodeDescriptionMap.put(PACKET_MALFORMED,
				"Error: update request packet malformed");
		packetCodeDescriptionMap.put(PACKET_MALFORMED_TOO_SHORT,
				"Error: update request packet malformed: too short");
		packetCodeDescriptionMap.put(PACKET_MALFORMED_WRONG_REQ_ELEM_NUMBER,
				"Error: update request packet malformed: wrong number " +
				"of request elements");
		
		// RE error codes
		reCodeDescriptionMap.put(RE_SUCCESS, "Success");
		reCodeDescriptionMap.put(RE_INVALID_OPCODE, "Error: invalid opcode");
		reCodeDescriptionMap.put(RE_INVALID_SUBDOMAIN,
				"Error: invalid subdomain");
		reCodeDescriptionMap.put(RE_INVALID_SUBDOMAIN_TOO_LONG,
				"Error: invalid subdomain: too long");
		reCodeDescriptionMap.put(RE_INVALID_SUBDOMAIN_INVALID_CHARS,
				"Error: invalid subdomain: contained invalid characters");
		reCodeDescriptionMap.put(RE_INVALID_RR_TYPE, "Error: invalid RR type");
		reCodeDescriptionMap.put(RE_INVALID_RR_TYPE_UNSUPPORTED,
				"Error: invalid RR type: given RR type unsupported");
		reCodeDescriptionMap.put(RE_INVALID_RR_DATA, "Error: invalid RR data");
		reCodeDescriptionMap.put(RE_INVALID_RR_DATA_COULD_NOT_FIND,
				"Error: invalid RR data: expected RR data, " +
				"but it was not found");
		reCodeDescriptionMap.put(RE_INVALID_TTL,
				"Error: invalid TTL");
		reCodeDescriptionMap.put(RE_OTHER_ERROR,
				"Error: could not process due to another error");
		reCodeDescriptionMap.put(RE_OTHER_ERROR_PACKET,
				"Error: could not process due to an error in the packet");
		reCodeDescriptionMap.put(RE_OTHER_ERROR_RE,
				"Error: could not process due to an error in another " +
				"request element");
		
	}
	
	public static String getPacketDescription(short code) {
		
		if (packetCodeDescriptionMap == null)
			buildCodeDescriptionMaps();
		
		if (packetCodeDescriptionMap.containsKey(code))
			return packetCodeDescriptionMap.get(code);
		else
			return "Error: unknown error";
		
	}
	
	public static String getREDescription(short code) {
		
		if (reCodeDescriptionMap == null)
			buildCodeDescriptionMaps();
		
		if (reCodeDescriptionMap.containsKey(code))
			return reCodeDescriptionMap.get(code);
		else
			return "Error: unknown error";
		
	}
	
	/*
	 * Default configuration options for the donar server.
	 */
	public static Properties getDefaultConfiguration() {
		Properties defaults = new Properties();
		defaults.setProperty("SERVER_LIST", "localhost:21001,");
		defaults.setProperty("CONFIG_RELOAD_INTERVAL", "100");
		defaults.setProperty("LOG_DIR", "/var/log/namecast/");
		defaults.setProperty("LOG_LEVEL", "DEBUG");
		defaults.setProperty("BACKEND", "MYSQL"); // Alternatives: {CRAQ, MYSQL}
		defaults.setProperty("CRAQ_HOST", "localhost");
		defaults.setProperty("CRAQ_PORT", "2727");
		defaults.setProperty("NUM_RECORDS_RETURNED", "3");
		return defaults;
	}

}
