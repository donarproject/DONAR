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

/* Attribute whose data is a double */
public class DoubleRecordAttribute extends RecordAttribute {
	public DoubleRecordAttribute(short typeID, double data) {
		this.typeID = typeID;
		this.data = data;
	}
	public DoubleRecordAttribute() {}
	public double data;
	public short getDataLength() { return 8; }
	public void writeData(DataOutputStream out) throws IOException {
		out.writeDouble(data);
	}
	public String toString() {
		return "Attribute " + this.typeID + "\t" + this.data;
	}
	public void setData(byte[] data) {
		long temp = (long)(data[0] &0xFF) << 56;
		temp += (long)(data[1] & 0xFF) << 48;
		temp += (long)(data[2] & 0xFF) << 40;
		temp += (long)(data[3] & 0xFF) << 32;
		temp += (long)(data[4] & 0xFF) << 24;
		temp += (long)(data[5] & 0xFF) << 16;
		temp += (long)(data[6] & 0xFF) << 8;
		temp += (long)(data[7] & 0xFF);
		this.data = Double.longBitsToDouble(temp);
	}
}
