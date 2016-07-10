/**
 * 
 */
package com.manager;

/**
 * @author nikhilchuramani
 *DB@ConManager is used to create a connection to the database.
 *The corresponding Connection variable is used to create a connection and getter
 *method can be used in different classes as needed. createConnection and closeConnection
 *methods are used to create a new connection and close it once the query has been
 *processed. 
 */

import java.sql.*;
public class DBConManager {
	private static DBConManager dbConManager = null;

	static final String JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver";  
	static final String DB_URL = "jdbc:db2://vsisls4.informatik.uni-hamburg.de:50001/VSISP";
	static final String USER = "vsisp15";
	static final String PASS = "eCULVnyP";
	
	private Connection conn;

	private DBConManager() {
		// TODO Auto-generated constructor stub
		try{

			Class.forName(JDBC_DRIVER);
		    conn = DriverManager.getConnection(DB_URL,USER,PASS);
		    conn.setAutoCommit(false);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	public void closeConnection(){
		try{
			conn.close();
		 }
		catch(Exception e){
			e.printStackTrace();
		 }
	}
	public static DBConManager getInstance() {
		dbConManager = new DBConManager();
	return dbConManager;
}
	public Connection getConnection(){
		return this.conn;
	}
}
