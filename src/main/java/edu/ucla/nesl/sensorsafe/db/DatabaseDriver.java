package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;

public interface DatabaseDriver {
	
	public void connect() 
			throws SQLException, ClassNotFoundException;
	
	public void close() 
			throws SQLException;
}
