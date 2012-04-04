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
import com.protomatter.syslog.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Since we rely on the somewhat shaky CRAQ code to provide storage, we
 * prefer to have a backup of key-hash: domain suffix pairs. That way if
 * CRAQ dies we can always recover account information and let our users
 * re-populate the data on their own.
 */
public class BackupStorage {
  // Constants
  private static final String MYSQL_SERVER_ADDR = "MYSQL_READ_SERVER";
  private static final String MYSQL_DB = "MYSQL_DB";
  private static final String MYSQL_USER = "MYSQL_USER";
  private static final String MYSQL_PASS = "MYSQL_PASS";
  private static final int NUM_CONNECTIONS = 5;
  private Connection dbConnection;
  static enum InstructionType { ADD, DELETE };
  private BlockingQueue<Instruction> outstanding;
  
  public BackupStorage() {	
    outstanding = new LinkedBlockingQueue<Instruction>();
	for (int i = 0; i < NUM_CONNECTIONS; i++) {
      Thread t = new Thread(new StoreRunnable(outstanding));
      t.start();
	}
  }
  
  private class Instruction {
    public InstructionType type;
    public String keyHash;
    public String suffix;
	 
    public Instruction(InstructionType type, String keyHash, String suffix) {
      this.type = type;
      this.keyHash = keyHash;
      this.suffix = suffix;
    }
  }
  
  private class StoreRunnable implements Runnable {
	private Connection dbConnection;
	private BlockingQueue<Instruction> instructions;
	  
    public StoreRunnable(BlockingQueue<Instruction> ins) {
      this.instructions = ins;
      try {
	    this.dbConnection = DriverManager.getConnection("jdbc:mysql://" + 
		  MYSQL_SERVER_ADDR + "/" + MYSQL_DB + "?user=" + 
		  MYSQL_USER + "&password=" + MYSQL_PASS);
		} catch (SQLException e) {
	  }
    }
	  
	@Override
	public void run() {
	  while (true) {
		Instruction in;
		try {
			in = instructions.take();
		} catch (InterruptedException e1) {
			continue;
		}
		try {
		  Statement stmt = this.dbConnection.createStatement();
		  ResultSet rs = stmt.executeQuery("SELECT * FROM " +
		  "backup WHERE key_hash = '" + in.keyHash + "'");
		  // If key not seen before, add to db
		  if (!rs.next()) {
            if (in.type == InstructionType.DELETE) { 
            	rs.close();
            	stmt.close();
            	continue; }
		    stmt.executeUpdate("INSERT INTO backup " +
		    "(key_hash, name_suffix) VALUES " +
		    "('" + in.keyHash + "','" + in.suffix + "')");
		  }
		  // We have a record
		  else {
		    if (in.type == InstructionType.ADD) {
		    	if (!rs.getString("name_suffix").equals(in.suffix)) {
		          stmt.execute("UPDATE backup set name_suffix='" + in.suffix +"'" +
		          		" where key_hash='" + in.keyHash + "'");
		    	}
		    	rs.close();
		    	stmt.close();
		    	continue; }
		    stmt.execute("DELETE FROM backup where key_hash='" +
			  in.keyHash + "' and name_suffix='" + in.suffix + "'");
		  }
		  rs.close();
		  stmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
	    }
	  }
    }
  }
  
  /**
   * Assure that we have the link between keyHash and suffix stored somewhere
   * permanent.
   * @param keyHash
   * @param suffix
 * @throws IOException 
   */
  public void assureStored(String keyHash, String suffix) throws IOException {
    Instruction toAdd = new Instruction(InstructionType.ADD, keyHash, suffix);
    this.outstanding.add(toAdd);
    Syslog.debug(this, "Request queued for [add] " + keyHash + " = " + suffix);
  }
  
  /**
   * Delete the link between keyHash and suffix.
   */
  public void delete(String keyHash, String suffix) throws IOException {
    Instruction toDelete = new Instruction(InstructionType.DELETE, keyHash, suffix);
	this.outstanding.add(toDelete);
    Syslog.debug(this, "Request queued for delete " + keyHash + " = " + suffix);
  }
  
  /**
   * Get all accounts that we are aware of. Used to recover from failures.	
   */
  public Map<String, String> getAllAccounts() throws IOException {
    Map<String, String> out = new HashMap<String, String>();
	Statement stmt;
	ResultSet rs = null;
	try {
		this.dbConnection = DriverManager.getConnection("jdbc:mysql://" +
				MYSQL_SERVER_ADDR + "/" + MYSQL_DB + "?user=" + 
				MYSQL_USER + "&password=" + MYSQL_PASS);
		stmt = this.dbConnection.createStatement();
	    rs = stmt.executeQuery("SELECT * FROM backup");
	    while (rs.next()) {
	    	String keyHash = rs.getString("key_hash");
	    	String domain = rs.getString("suffix");
	    	out.put(keyHash, domain);
	    }
	    rs.close();
	    stmt.close();
	} catch (SQLException e) {
		throw new IOException("Error talking to MySQL database");
	}
    return out;
  }
  public static void main(String[] args) throws IOException {
	// Informal unit tests :)
    BackupStorage myStorage = new BackupStorage();
    List<String> stored = new LinkedList<String>();
    
    for (int i = 0; i < 50; i++) {
    	byte[] b = new byte[43];
    	(new Random()).nextBytes(b);
    	String s = new String(b);
    	myStorage.assureStored(s, s);
    	stored.add(s);
    }
    try {
		Thread.sleep(1000 * 50);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    for (String s: stored) {
    	myStorage.delete(s, s);
    }
  }
} 


