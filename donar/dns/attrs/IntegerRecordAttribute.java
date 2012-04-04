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

/* Attribute whose data is an integer */
public class IntegerRecordAttribute extends RecordAttribute {
	public IntegerRecordAttribute(short typeID, int data) {
		this.data = data;
		this.typeID = typeID;
	}
	public IntegerRecordAttribute() {}
	public int data;
	public short getDataLength() { return 4; }
	public void writeData(DataOutputStream out) throws IOException {
		out.writeInt(data);
	}
	public String toString() {
		return "Attribute " + this.typeID + "\t" + this.data;
	}
	public void setData(byte[] data) {
		this.data = (data[0] &0xFF) << 24;
		this.data += (data[1] & 0xFF) << 16;
		this.data += (data[2] & 0xFF) << 8;;
		this.data += (data[3] & 0xFF);
	}
}