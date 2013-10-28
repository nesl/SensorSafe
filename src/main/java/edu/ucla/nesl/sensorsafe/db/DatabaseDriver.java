package edu.ucla.nesl.sensorsafe.db;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;

public interface DatabaseDriver {
	
	public void connect() 
			throws SQLException, ClassNotFoundException, IOException, NamingException;
	
	public void close() 
			throws SQLException;
}
