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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.protomatter.syslog.Syslog;

import donar.dns.attrs.IntegerRecordAttribute;
import donar.dns.attrs.RecordAttribute;
import donar.update.UpdateInfo;

public class MySQLBackend implements DONARBackend {

	// Constants
	private static final String MYSQL_READ_SERVER_ADDR = "localhost";
	private static final String MYSQL_WRITE_SERVER_ADDR = 
		"MYSQL_READ_SERVER";
	private static final String MYSQL_DB = "MYSQL_DB";
	private static final String MYSQL_USER = "MYSQL_USER";
	private static final String	MYSQL_PASS = "MYSQL_PASS";
	
	private Connection localdbConnection;
	private ConnectionProvider cp;
	private String acctKeyHash;
	private String acctSequenceNum;
	private String acctNameSuffix;
	private int domainID; // MySQL domain record ID
	
	public static void main(String[] args) {
		try {
			MySQLBackend test = new MySQLBackend();
			test.answerQuery("foo.com", "IN", "A", "-1", "63.20.254.10");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public MySQLBackend() throws IOException {
		
		
		try {
		  Properties props = new Properties();
		  cp = new ConnectionProvider("com.mysql.jdbc.Driver",
		      "jdbc:mysql://" + MYSQL_WRITE_SERVER_ADDR + "/" + MYSQL_DB,
		      MYSQL_USER, MYSQL_PASS);
	  
		  Syslog.info(this, "Created connection pool");
			// The below function (getConnection) just hangs if mysql
			// is down on the write server. So we need to induce an exception
			// here if the server is down.
			Socket test = new Socket();
	        test.connect(new InetSocketAddress(
	        		InetAddress.getByName(MYSQL_WRITE_SERVER_ADDR), 3306), 
	        		10 * 1000);
	        test.close();

			Syslog.info(this, "Established connection to MYSQL on " +
				MYSQL_WRITE_SERVER_ADDR);
			
		}
		// Continue on if write server connection is broken, at least
		// we can still read. Though write calls will all fail.
		// TODO: Make prettier.
		catch (Exception e) {
			System.out.println(e);
			Syslog.log(this, e, Syslog.ERROR);
		}
		
		try {
			localdbConnection = DriverManager.getConnection("jdbc:mysql://" + 
					MYSQL_READ_SERVER_ADDR + "/" + MYSQL_DB + "?user=" + 
					MYSQL_USER + "&password=" + MYSQL_PASS);
			
			Syslog.info(this, "Established connections to MYSQL on " +
				MYSQL_READ_SERVER_ADDR + " and " +
				MYSQL_WRITE_SERVER_ADDR);
		}
		catch (Exception e) {
			System.out.println(e);
			Syslog.log(this, e, Syslog.FATAL);
			throw new IOException("Error initializing MySQL connection:" +
					e.getMessage());
		}
	}
	
	/* 
	 * Adds a DONAR record. If the subdomain
	 * string is blank, adds a record for the suffex itself. If record exists with same
	 * subdomain, type and content, replaces data (ttl/atributes).
	 * */
	public void addRecord(String subdomain, String type, String content, int ttl,
			List<RecordAttribute> attributes)
			throws IOException {
		String fqdn;
		if (subdomain.equals("")) {
			fqdn = acctNameSuffix;
		}
		else {
			fqdn = subdomain + "." + acctNameSuffix;
		}
		
		try {
			 PreparedStatement ps = null;
			 Connection dbConnection = cp.connection();
			 Statement stmt = dbConnection.createStatement();
			 String query = "SELECT * FROM records" + " WHERE domain_id=" + this.domainID + " AND" +
					 " name='" + fqdn + "' AND content='" + content + "'";
			 Syslog.debug(this, "Executing SQL statement: " + query);
			 ResultSet rs = stmt.executeQuery(query);
			 if (rs.next()) {
				 // There is already a record
				 int id = rs.getInt(1);
				 ps = dbConnection.prepareStatement(
		                 "UPDATE records set ttl=?, expires=FROM_UNIXTIME(?) where id=?");
				 ps.setInt(1, ttl);
				 ps.setInt(3, id);
				 boolean setExpires = false;
				 if (attributes == null) ps.setNull(2, java.sql.Types.INTEGER);
				 else {
					 for (RecordAttribute att: attributes) {
						 if (att.typeID == UpdateInfo.EXPIRATION_TIME) {
							 int stamp = ((IntegerRecordAttribute) att).data;
							 ps.setInt(2, stamp);
							 setExpires = true;
						 }
					 }
				 }
				 if (!setExpires) ps.setNull(2, java.sql.Types.INTEGER);
				 Syslog.debug(this, "Executing SQL statement: " +
		                 ps.toString());
				 ps.executeUpdate();
				 ps.close();
				 rs.close();
				 stmt.close();
				 dbConnection.close();
				 return;
			 }
			 rs.close();
			 stmt.close();
			 ps = dbConnection.prepareStatement(
	                 "INSERT INTO records (domain_id, name, content, " +
	                 "type, ttl, prio, expires) VALUES (?,?,?,?,?,?," +
	                 "FROM_UNIXTIME(?))");
			 ps.setInt(1, this.domainID);
			 ps.setString(2, fqdn);
			 ps.setString(3, content);
			 ps.setString(4, type);
			 ps.setInt(5, ttl); 
			 ps.setNull(6, java.sql.Types.INTEGER); // TODO Fix
			 //Integer prio = getPrio(re);
			 //if (prio == null)
			 //       ps.setNull(6, java.sql.Types.INTEGER);
			 //else
			 //        ps.setInt(6, getPrio(re));
			 
			 // Set expire time if specified
			 boolean setExpires = false;
			 if (attributes == null) ps.setNull(7, java.sql.Types.INTEGER);
			 else {
				 for (RecordAttribute att: attributes) {
					 if (att.typeID == UpdateInfo.EXPIRATION_TIME) {
						 int stamp = ((IntegerRecordAttribute) att).data;
						 ps.setInt(7, stamp);
						 setExpires = true;
					 }
				 }
			 }
			 if (!setExpires) ps.setNull(7, java.sql.Types.INTEGER);
			 
			 Syslog.debug(this, "Executing SQL statement: " +
			                 ps.toString());
			 ps.executeUpdate();
			 ps.close();
			 dbConnection.close();
		}
		catch (SQLException e) {
			Syslog.log(this, e, Syslog.WARNING);
			throw new IOException("Error adding record to MySQL database: " +
					e.getMessage());
		}

	}

	public List<DNSRecord> answerQuery(String qname, String qclass, String qtype,
			String id, String remoteIPAddress) throws IOException {
		List<DNSRecord> out = new LinkedList<DNSRecord>();
		try {
			Statement st = localdbConnection.createStatement();
		    String query = "";
		    ResultSet rs;
			if (qtype.equals("ANY")) {
				query = "SELECT * FROM records WHERE name='" + qname +
						"' AND ((expires is NULL) OR " +
						"(expires > (CONVERT_TZ(NOW(), @@global.time_zone, 'GMT'))))";
				rs = st.executeQuery(query);
				
				// If there are no results fail over to expired records
				if (!rs.next()){ 
					query = "SELECT * FROM records WHERE name='" + qname + "'"; 
					rs = st.executeQuery(query);
				}
				else {
					rs.previous();
				}
			}
			else {
				query = "SELECT * FROM records WHERE name='" + qname + 
					"' AND type='" + qtype + 
					"' AND ((expires is NULL) OR " +
					"(expires > (CONVERT_TZ(NOW(), @@global.time_zone, 'GMT'))))";
				rs = st.executeQuery(query);
				
				// If there are no results fail over to expired records
				if (!rs.next()) {
					query = "SELECT * FROM records WHERE name='" + qname + 
					"' AND type='" + qtype + "'";
					rs = st.executeQuery(query);					
				}
				else {
					rs.previous();
				}
			}
			
			while (rs.next()) {
				DNSRecord toAdd = new DNSRecord(rs.getString("type"), 
						rs.getString("content"), rs.getInt("ttl"), 
						new LinkedList<RecordAttribute>());
				out.add(toAdd);
			}
			
			rs.close();
			st.close();
		}
		catch (SQLException e) {
			throw new IOException("Error talking to MySQL database");
		}
		
		return out;
	}

	public void assureKey(String keyHash) throws IOException {
        // See if this key is in our database, if not create it.
		try {
		  Connection dbConnection = cp.connection();
			Statement stmt = dbConnection.createStatement();
	        ResultSet rs = stmt.executeQuery("SELECT key_hash FROM " +
	                        "key_info WHERE key_hash = '" + keyHash + "'");
	        // If key not seen before, add to db with defaults
	        if (!rs.next()) {
	                stmt.executeUpdate("INSERT INTO key_info " +
	                   "(key_hash, next_sequence_num, name_suffix) VALUES " +
	                   "('" + keyHash + "',1,'" + keyHash + ".donardns.net')");
	                stmt.executeUpdate("INSERT INTO domains (name, type) " +
	                		"VALUES ('" + keyHash + 
	                		".donardns.net', 'NATIVE')");
	        }
	        rs.close();
	        stmt.close();
	        dbConnection.close();
		}
		catch (SQLException e) {
			throw new IOException("Error talking to MySQL database");
		}
		
	}

	public void bindAccount(String keyHash) throws IOException {
		// First try to lookup domain name and sequence number
		// in namecast table. Then lookup domain ID in domains table
		// and store for future queries.
		try {
		  Connection dbConnection = cp.connection();
		  Statement stmt = dbConnection.createStatement();
	      ResultSet rs = stmt.executeQuery("SELECT * FROM " +
	                      "key_info WHERE key_hash = '" + keyHash + "'");
	      if (rs.next()) {
	              this.acctNameSuffix = rs.getString("name_suffix");
	              this.acctKeyHash = rs.getString("key_hash");
	              this.acctSequenceNum = rs.getString("next_sequence_num");
	              
	              ResultSet rs2 = stmt.executeQuery("SELECT * FROM " +
	                      "domains WHERE name = '" + this.acctNameSuffix + "'");
	              if (rs2.next()) {
	            	  this.domainID = rs2.getInt("id");
	            	  rs2.close();
	              }
	              else { // No domain
	            	  rs2.close();
	            	  throw new SQLException("Could not find info for domain " +
		            		  this.acctNameSuffix + " in database");
	              }
	      } else { // No DONAR record
	              throw new SQLException("Could not find info for key " +
	            		  keyHash + " in database");
	      }
	      
	      rs.close();
	      stmt.close();
	      dbConnection.close();
		}
		catch(SQLException e) {
			throw new IOException("Error binding account: " + e.getMessage());
		}


	}

	public void delRecords(String subdomain, String type, String content)
			throws IOException {
		// Get full domain name
		String fqdn;
		if (subdomain.equals("")) {
			fqdn = acctNameSuffix;
		}
		else {
			fqdn = subdomain + "." + acctNameSuffix;
		}
		// Delete subdomains from records table
		PreparedStatement ps;
		try {
		  Connection dbConnection = cp.connection();
		    if (type.equals("")) {
		            // Delete all records for subdomain
		            ps = dbConnection.prepareStatement(
		            		"DELETE FROM records WHERE name = ?");
		            ps.setString(1, fqdn);
		    } else if (content.equals("")) {
		            // Delete all records of given type for subdomain
		            ps = dbConnection.prepareStatement(
		            		"DELETE FROM records WHERE " +
		                    "name = ? AND type = ?");
		            ps.setString(1, fqdn);
		            ps.setString(2, type);
		    } else {
		            // Delete all records of given type with given
		            // data for subdomain
		            ps = dbConnection.prepareStatement(
		            		"DELETE FROM records WHERE " +
		                    "name = ? AND type = ? AND content = ?");
		            ps.setString(1, fqdn);
		            ps.setString(2, type);
		            ps.setString(3, content);
		    }
		    Syslog.debug(this, "Executing SQL query: " + ps.toString());
			ps.executeUpdate();
			ps.close();
			dbConnection.close();
		}
		catch(SQLException e) {
			throw new IOException("Error deleting records: " + e.getMessage());
		}

	}

	public long getSequenceNum(String keyHash) throws IOException {
		// Lookup sequence number in key_info table
		try {
		  Connection dbConnection = cp.connection();
			Statement stmt = dbConnection.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT * FROM " +
		    	"key_info WHERE key_hash = '" + keyHash + "'");
	    	if (rs.next()) {
	    		long temp =  rs.getLong("next_sequence_num");
		    	rs.close();
		    	stmt.close();
		    	dbConnection.close();
	    		return temp;
	    	}
	    	rs.close();
	    	stmt.close();
	    	dbConnection.close();
		}
		catch(SQLException e) {	
			throw new IOException("Error getting sequence number: "
					+ e.getMessage());
		}
		
		return -1; // Shouldn't ever get here
	}

	public void incrementSequenceNum() throws IOException {
		// Get this record and update it
		try {
      Connection dbConnection = cp.connection();
			Statement stmt = dbConnection.createStatement();
	        ResultSet rs = stmt.executeQuery("SELECT next_sequence_num FROM " +
	                        "key_info WHERE key_hash = '" +
	                        this.acctKeyHash + "'");
	        if (rs.next()) {
	                stmt.executeUpdate("UPDATE key_info SET " +
	                                "next_sequence_num=next_sequence_num+1 " +
	                                "WHERE key_hash = '" +
	                                this.acctKeyHash + "'");
	        }
	        rs.close();
	        stmt.close();
	        dbConnection.close();
		}
        catch(SQLException e) {
			throw new IOException("Error incrementing sequence number: "
					+ e.getMessage());
		}
	}

	public void unbindAccount(String keyHash) throws IOException {
		this.acctKeyHash = null;
		this.acctSequenceNum = null;
		this.acctNameSuffix = null;
		this.domainID = -1;
	}

	public void updateSuffix(String newSuffix) throws IOException {
		PreparedStatement ps;
		// Update all three relevant tables to new domain suffix
		try {
			// key_info table
      Connection dbConnection = cp.connection();
			ps = dbConnection.prepareStatement(
	        	"UPDATE key_info SET name_suffix = ? WHERE key_hash = ?");
			ps.setString(1, newSuffix);
			ps.setString(2, this.acctKeyHash);
		    Syslog.debug(this, "Executing SQL query: " + ps.toString());
			ps.executeUpdate();
			ps.close();
			
			// records table
			ps = dbConnection.prepareStatement(
        	"UPDATE records SET name = REPLACE(name, ?, ?)" +
        	" WHERE domain_id = ?");
			ps.setString(1, this.acctNameSuffix);
			ps.setString(2, newSuffix);
			ps.setLong(3, this.domainID);
		    Syslog.debug(this, "Executing SQL query: " + ps.toString());
			ps.executeUpdate();
			ps.close();
			
			// domains table
			ps = dbConnection.prepareStatement(
        	"UPDATE domains SET name = ? WHERE id = ?");
			ps.setString(1, newSuffix);
			ps.setLong(2, this.domainID);
		    Syslog.debug(this, "Executing SQL query: " + ps.toString());
			ps.executeUpdate();
			ps.close();
			dbConnection.close();
			
		}
		catch(SQLException e) {
			System.out.println(e.getMessage());
			throw new IOException("Error changing suffix number: "
					+ e.getMessage());
		}
		
		// Update cached suffix
		this.acctNameSuffix = newSuffix;

	}

	@Override
	public String getSuffix() {
		return this.acctNameSuffix;
	}
}