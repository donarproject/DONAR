package donar.dns.attrs;

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

/* Class to store attributes for records */
public abstract class RecordAttribute {
	public short typeID;
	
	public abstract short getDataLength();
	
	protected abstract void writeData(DataOutputStream out) throws IOException;
	
	public void writeXDR(DataOutputStream out) throws IOException {
		out.writeInt(typeID);
		out.writeInt((int) getDataLength());
		writeData(out);
	}
	
	public void writeRecord(DataOutputStream out) throws IOException {
		out.writeShort(typeID);
		out.writeShort(getDataLength());
		writeData(out);
	}
	
	/* Sets local varaible (double, int, etc.) from passed in bytes */
	public abstract void setData(byte[] data);
}