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

import java.io.*;
import java.net.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;

import com.protomatter.syslog.*;

import donar.dns.attrs.*;
import donar.update.util.*;


public class UpdateServerThread implements Runnable  {

	private DatagramPacket packet;
	private byte[] packetBytes;
	private DONARBackend backend;
	private DatagramSocket socket;
	private BackupStorage bs;
	
	
	public UpdateServerThread(DatagramPacket packet, DatagramSocket socket, 
			DONARBackend backend, BackupStorage bs)
		throws IOException
	{
		this.packet = packet;
		this.socket = socket;
		this.packetBytes = new byte[packet.getLength()];
		for (int i = 0; i < packet.getLength(); i++) {
			this.packetBytes[i] = packet.getData()[i];
		}
		this.backend = backend;
		this.bs = bs;
		Syslog.info(this, "Update thread successfully connected to backend");
	}
	
	public void run()
	{
		ByteArrayInputStream byteStream = new ByteArrayInputStream(packetBytes);
		DataInputStream data = new DataInputStream(byteStream);
		
		try {
			// Verify magic string
		    byte[] magic = new byte[UpdateInfo.NUP_MAGIC_STRING.length];
			data.readFully(magic);
			if (!verifyMagic(magic)) {
				Syslog.debug(this, "Aborting: packet not NUP");
				return; // Abort if this is not a NUP packet
			}
			
			// Verify version
			short version = data.readShort();
			if (!verifyVersion(version)) {
				Syslog.debug(this, "Aborting: packet NUP" +
						" version not supported");
				packetReply(packet, UpdateInfo.PACKET_NUP_VERSION_UNSUPPORTED);
				return;
			}
			
			// Read in remaining data
			short keyLen = data.readShort();
			byte[] publicKey = new byte[keyLen];
			data.readFully(publicKey);
			long sequenceNum = data.readLong();
			short reCount = data.readShort();
			
			RequestElement[] reArray = new RequestElement[reCount];
			for (int i = 0; i < reCount; i++) {
				
				short opcode = data.readShort();
				
				short subdomainLen = data.readShort();
				byte[] subdomainBytes = new byte[subdomainLen];
				data.readFully(subdomainBytes);
				String subdomain = new String(subdomainBytes);
				
				short rrTypeLen = data.readShort();
				byte[] rrTypeBytes = new byte[rrTypeLen];
				data.readFully(rrTypeBytes);
				String rrType = new String(rrTypeBytes);
				
				int rrDataLen = data.readInt();
				byte[] rrDataBytes = new byte[rrDataLen];
				data.readFully(rrDataBytes);
				
				int ttl = data.readInt();
				short numAttrs = data.readShort();
				RequestElement re = new RequestElement(opcode, subdomain,
						rrType, rrDataBytes, ttl);
				for (int j = 0; j < numAttrs; j++) {
					short attType = data.readShort();
					short attLength = data.readShort();
					byte[] attData = new byte[attLength];
					data.readFully(attData);
					RecordAttribute newAtt = null;
					switch (attType) {
						// Convert TTL into absolute time
						case UpdateInfo.DONAR_TTL: 
							newAtt = new IntegerRecordAttribute();
							long time = ((new Date()).getTime()) / 1000;
							newAtt.setData(attData);
							time = time + ((IntegerRecordAttribute) newAtt).data;
							int expires;
							expires = ((int) ((time >> 24) & 0xFF)) << 24;
							expires += ((int) ((time >> 16) & 0xFF)) << 16;
							expires += ((int) ((time >> 8) & 0xFF)) << 8;;
							expires += ((int) ((time) & 0xFF));
							newAtt.typeID = UpdateInfo.EXPIRATION_TIME;
							((IntegerRecordAttribute) newAtt).data = expires;
							attType = UpdateInfo.EXPIRATION_TIME;
							break;
							
						case UpdateInfo.SPLIT_PROPORTION:
							newAtt = new DoubleRecordAttribute();
							newAtt.setData(attData);
							break;
							
						case UpdateInfo.SPLIT_EPSILON:
							newAtt = new DoubleRecordAttribute();
							newAtt.setData(attData);
							break;
							
						case UpdateInfo.BANDWIDTH_CAP:
							newAtt = new DoubleRecordAttribute();
							newAtt.setData(attData);
							break;		
							
						case UpdateInfo.DIST_ADJUSTMENT:
							newAtt = new DoubleListRecordAttribute();
							newAtt.setData(attData);
							break;
						default: continue;
					}

					newAtt.typeID = attType;
					re.attributes.add(newAtt);
				}
				
				Syslog.debug(this, "Read request element from packet", re);
				
				reArray[i] = re;
			}
			
			short signatureLen = data.readShort();
			byte[] signature = new byte[signatureLen];
			data.readFully(signature);
			
			// Verify signature
			if (!verifySignature(publicKey, signature, packetBytes,
					0, packetBytes.length - 2 - signatureLen)) {
				Syslog.debug(this, "Signature invalid. Sending reply packet.");
				packetReply(packet, publicKey, sequenceNum,
						UpdateInfo.PACKET_INVALID_SIGNATURE);
				return;
			}
			Syslog.debug(this, "Packet signature valid.");
			
			// Add key to database if we haven't seen it before
			String keyHash = KeyUtil.getHashString(publicKey);
			backend.assureKey(keyHash);
			Syslog.debug(this, "AssureKey complete for  " + keyHash);
			
			
			/*
			// Verify sequence number
			if (!verifySequenceNum(sequenceNum, publicKey))	 {
				Syslog.debug(this, "Sequence nubmber invalid. Sending " +
						"reply packet.");
				packetReply(packet, publicKey, sequenceNum,
						UpdateInfo.PACKET_INVALID_SEQNUM);
				return;
			}
			Syslog.debug(this, "Sequence number verified.");
			*/
			
			// Process request
			List<Short> codes = processRequest(reArray, publicKey);
			
			Syslog.debug(this, "Sending reply packet.");
			packetReply(packet, publicKey, sequenceNum, codes);
			
		} catch (IOException e) {
			Syslog.error(this, "Failure processing packet: " + e.getMessage());
			e.printStackTrace();
			packetReply(packet, UpdateInfo.PACKET_FAILURE);
			return;
		}
		
	}
	
	
	/*
	 * Given an array of request elements decoded from a NUP packet, 
	 * attempt to process those requests. If request update fails, 
	 * indicate failure reason in the opcode field of the request element.
	 */
	private List<Short> processRequest(RequestElement[] reArray, 
			byte[] publicKey)
	throws IOException
	{
		Syslog.debug(this, "Beginning to handle request elements in packet.");
		
		String keyHash = KeyUtil.getHashString(publicKey);
		short mainCode = UpdateInfo.PACKET_FAILURE_NO_RE;
		LinkedList<Short> codeList = new LinkedList<Short>();
		
		// Get AccountInfo to update
		backend.bindAccount(keyHash);
		Syslog.debug(this, "Bound account to backend: " + keyHash);
		
		if (bs != null) {
	      bs.assureStored(keyHash, backend.getSuffix());
		}
		
		for (RequestElement re: reArray) {
			// Process individual REs
			
			short reCode = re.checkRE();
			
			if (reCode == UpdateInfo.RE_SUCCESS) {
				
				// Ask backend to handle this request
				try {
					if (re.opcode == UpdateInfo.NUP_OPCODE_ADD) {
						Syslog.debug(this, "Adding record...");;
						backend.addRecord(re.subdomain,
								re.rrtype, re.rrdata, re.ttl, re.attributes);		
						Syslog.debug(this, "Succesfully added new record for " + 
								re.subdomain);
					} else if (re.opcode == UpdateInfo.NUP_OPCODE_DELETE) {
						
						backend.delRecords(re.subdomain, re.rrtype, re.rrdata);
						Syslog.debug(this, "Succesfully deleted record for " + 
								re.subdomain);
						
					} else if (re.opcode == UpdateInfo.NUP_OPCODE_VALIDATE) {
						if (KeyUtil.validateDomain(re.subdomain, keyHash)) { 
							String newSuffix = re.subdomain;
							Syslog.debug(this, "updating suffix for " + 
									re.subdomain);
							backend.updateSuffix(newSuffix);
							
							backend.addRecord("", "SOA", "localhost " + re.rrdata + " 0", re.ttl,
								re.attributes);
						} else {
							throw new IOException("Domain " + re.subdomain +
									" could not be validated for key " +
									keyHash);
						}
					}
	
					// Update main code for success on this RE
					if (mainCode == UpdateInfo.PACKET_FAILURE_NO_RE)
						mainCode = UpdateInfo.PACKET_SUCCESS;
					else if (mainCode == UpdateInfo.PACKET_FAILURE)
						mainCode = UpdateInfo.PACKET_PARTIAL_SUCCESS;	
				} catch (IOException e) {
					// handle IO failure
					Syslog.error(this, "Error processing request: " + e.getMessage());
					reCode = UpdateInfo.RE_OTHER_ERROR;
					if (mainCode == UpdateInfo.PACKET_FAILURE_NO_RE)
						mainCode = UpdateInfo.PACKET_FAILURE;
					else if (mainCode == UpdateInfo.PACKET_SUCCESS)
						mainCode = UpdateInfo.PACKET_PARTIAL_SUCCESS;
				}
				
			} else {
				Syslog.debug(this, "No request element found");
				// Update main code for failure on this RE
				if (mainCode == UpdateInfo.PACKET_FAILURE_NO_RE)
					mainCode = UpdateInfo.PACKET_FAILURE;
				else if (mainCode == UpdateInfo.PACKET_SUCCESS)
					mainCode = UpdateInfo.PACKET_PARTIAL_SUCCESS;
			}
			
			// Add RE-specific code to list
			codeList.addLast(reCode);
		}
		
		// Increment sequence number if necessary
		if (mainCode == UpdateInfo.PACKET_SUCCESS ||
			mainCode == UpdateInfo.PACKET_PARTIAL_SUCCESS)
			backend.incrementSequenceNum();
	
		backend.unbindAccount(keyHash);
		Syslog.debug(this, "Handled requests and unbound account from backend.");
		
		codeList.addFirst(mainCode); // Add main status code
		return codeList;
		
	}
	
	private void packetReply(DatagramPacket packet, short errorCode)
	{
		// Reply with no public key or sequence number
		try {
			
			// build reply
			ByteArrayOutputStream replyBytes = new ByteArrayOutputStream();
			DataOutputStream replyData = new DataOutputStream(replyBytes);
			replyData.write(UpdateInfo.NUP_MAGIC_STRING);
			replyData.writeShort(UpdateInfo.NUP_VERSION);
			replyData.writeShort(0); // No public key
			replyData.writeLong(-1); // No sequence number
			replyData.writeLong(-1);
			replyData.writeShort(0); // No RE codes
			replyData.writeShort(errorCode);
			replyData.flush();
			
			// send reply
			SocketAddress replyAddr = packet.getSocketAddress();
			byte[] replyArray = replyBytes.toByteArray();
			DatagramPacket replyPacket =
				new DatagramPacket(replyArray, replyArray.length, replyAddr);
			this.socket.send(replyPacket);
			Syslog.debug(this, "Send reply packet to: " + replyAddr);
			
		} catch (IOException e) {
			Syslog.warning(this, "Error sending packet reply", e);
			return;
		}
		
	}
	
	private void packetReply(DatagramPacket packet, byte[] publicKey,
			 long sequenceNum, short errorCode)
	{
		List<Short> codeList = new LinkedList<Short>();
		codeList.add(errorCode);
		packetReply(packet, publicKey, sequenceNum, codeList);
	}
	
	private void packetReply(DatagramPacket packet, byte[] publicKey,
							 long sequenceNum, List<Short> errorCodes) {
		// Reply with everything
		
		try {
			
			long nextSequenceNum =
				backend.getSequenceNum(KeyUtil.getHashString(publicKey));
			
			// build reply
			ByteArrayOutputStream replyBytes = new ByteArrayOutputStream();
			DataOutputStream replyData = new DataOutputStream(replyBytes);
			replyData.write(UpdateInfo.NUP_MAGIC_STRING);
			replyData.writeShort(UpdateInfo.NUP_VERSION);
			replyData.writeShort(publicKey.length);
			replyData.write(publicKey);
			replyData.writeLong(sequenceNum);
			replyData.writeLong(nextSequenceNum);
			replyData.writeShort(errorCodes.size() - 1);
			for (short code : errorCodes)
				replyData.writeShort(code);
			replyData.flush();
			
			// send reply
			SocketAddress replyAddr = packet.getSocketAddress();
			byte[] replyArray = replyBytes.toByteArray();
			DatagramPacket replyPacket =
				new DatagramPacket(replyArray, replyArray.length, replyAddr);
			this.socket.send(replyPacket);
			Syslog.debug(this, "Send reply packet to: " + replyAddr);
			
		} catch (IOException e) {
			// Just don't send a reply on failure
			Syslog.warning(this, "Error sending packet reply", e);
			return;
		}
	}
	
	private boolean verifyMagic(byte[] magic)
	{
		return Arrays.equals(magic, UpdateInfo.NUP_MAGIC_STRING);
	}
	
	private boolean verifyVersion(short version)
	{
		return version == UpdateInfo.NUP_VERSION;
	}
	
	private boolean verifySignature(byte[] publicKey, byte[] signature,
			byte[] signedData, int rangeStart, int rangeEnd)
	{
		try {
			KeyFactory keyFactory = KeyFactory.getInstance("DSA");
			X509EncodedKeySpec pubSpec = new X509EncodedKeySpec(publicKey);
			PublicKey pubKey = keyFactory.generatePublic(pubSpec);
			Signature verifier = Signature.getInstance("DSA");
			verifier.initVerify(pubKey);
			verifier.update(signedData, rangeStart, rangeEnd);
			return verifier.verify(signature);
		} catch (InvalidKeyException e) {
			// Fail to verify on exception
			return false;
		} catch (NoSuchAlgorithmException e) {
			return false;
		} catch (SignatureException e) {
			return false;
		} catch (InvalidKeySpecException e) {
			return false;
		}
		
	}
	
	
	private boolean verifySequenceNum(long sequenceNum, byte[] publicKey)
	throws IOException
	{
		String hashStr = KeyUtil.getHashString(publicKey);
		long correctNum = backend.getSequenceNum(hashStr);
		if (correctNum == sequenceNum)
			return true;
		else
			Syslog.debug(this, "Invalid seq num: Expecting " + correctNum + 
					" and found " + sequenceNum);
			return false;
	}

}
