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

/* Attribute whose data is a list of doubles */
public class DoubleListRecordAttribute extends RecordAttribute {
	public DoubleListRecordAttribute(short typeID, double[] data) {
		this.typeID = typeID;
		this.data = data;
	}
	public DoubleListRecordAttribute() {}
	public double[] data;
	public short getDataLength() { 
		return (short) (8 * data.length); 
	}
	public void writeData(DataOutputStream out) throws IOException {
		for (double d : data) {
			out.writeDouble(d);
		}
	}
	public String toString() {
		return "Attribute " + this.typeID + "\t" + this.data;
	}
	public void setData(byte[] data) {
		int numDoubles = data.length / 8;
		this.data = new double[numDoubles];
		for (int i = 0; i < numDoubles; i++) {
			long temp = (long)(data[(i * 8) + 0] &0xFF) << 56;
			temp += (long)(data[(i * 8) + 1] & 0xFF) << 48;
			temp += (long)(data[(i * 8) + 2] & 0xFF) << 40;
			temp += (long)(data[(i * 8) + 3] & 0xFF) << 32;
			temp += (long)(data[(i * 8) + 4] & 0xFF) << 24;
			temp += (long)(data[(i * 8) + 5] & 0xFF) << 16;
			temp += (long)(data[(i * 8) + 6] & 0xFF) << 8;
			temp += (long)(data[(i * 8) + 7] & 0xFF);
			this.data[i] = Double.longBitsToDouble(temp);
		}
	}
}
