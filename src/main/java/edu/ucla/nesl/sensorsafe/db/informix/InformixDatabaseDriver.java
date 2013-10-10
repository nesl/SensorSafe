package edu.ucla.nesl.sensorsafe.db.informix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import edu.ucla.nesl.sensorsafe.db.DatabaseDriver;

abstract public class InformixDatabaseDriver implements DatabaseDriver {

	// TODO Read this from properties file.
	private static final String HOST_SERVER_DNS_NAME = "ibmdb";
	private static final String HOST_SERVER_PORT = "24172";
	private static final String DB_SERVER_NAME = "sensorsafe";
	private static final String DB_NAME = "sensorsafe";
	private static final String DB_USERNAME = "informix";
	private static final String DB_PASSWORD = "sensorsafe!";

	protected static final String DB_CONNECT_URL = "jdbc:informix-sqli://" 
			+ HOST_SERVER_DNS_NAME + ":" + HOST_SERVER_PORT + "/" + DB_NAME +
			":INFORMIXSERVER=" + DB_SERVER_NAME + 
			";user=" + DB_USERNAME + 
			";password=" + DB_PASSWORD;

	protected Connection conn;

	@Override
	public void connect() throws SQLException, ClassNotFoundException {
		if (conn == null) {
			Class.forName("com.informix.jdbc.IfxDriver");
			conn = DriverManager.getConnection(DB_CONNECT_URL);
			initializeDatabase();
		}
	}

	@Override
	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}
	
	abstract protected void initializeDatabase() throws SQLException, ClassNotFoundException;
}