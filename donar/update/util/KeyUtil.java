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

import java.math.BigInteger;
import java.security.MessageDigest;

import donar.update.UpdateInfo;

public class KeyUtil {
	
	public static String getHashString(byte[] publicKey)
	{
		try {
			
			MessageDigest hasher = MessageDigest.getInstance("SHA1");
			hasher.update(publicKey);
			byte[] hash = hasher.digest();
			BigInteger hashInt = new BigInteger(1, hash);
			String hashStr = hashInt.toString(16);
			
			// Zero pad if necessary
			int zeroesNeeded =
				UpdateInfo.SHA1_HASH_BYTES * 2 - hashStr.length();
			for (int i = 0; i < zeroesNeeded; i++)
				hashStr = "0" + hashStr;
			
			return hashStr;
			
		} catch (Exception e) {
			// Fail on exception
			return null;
		}
	}
	
	public static boolean validateDomain(String name, String keyHash)
	{
		return true;
		/*
		try {
			Record [] records = new Lookup("validate-" + keyHash + "." + name, Type.CNAME).run();
			for (int i = 0; i < records.length; i++) {
				CNAMERecord record = (CNAMERecord) records[i];
				String cname = record.getAlias().toString();
				if (cname.equals("donardns.net"))
					return true;
			}
		} catch (Exception e) {
			return false; // Fail validation on error
		}
		return false;
		*/
	}

}
