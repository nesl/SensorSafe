package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;

import edu.ucla.nesl.sensorsafe.model.User;

public interface UserDatabaseDriver {
	
	public void connect() throws SQLException, ClassNotFoundException;
	
	public void close() throws SQLException;
	
	public void prepareAdminDatabase() throws SQLException, ClassNotFoundException;
	
	public void changeAdminPassword(String password) throws SQLException;

	public void registerOwner(User newOwner) throws SQLException;
	
}
