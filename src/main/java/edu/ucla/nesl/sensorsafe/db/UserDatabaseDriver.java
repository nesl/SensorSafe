package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;

import edu.ucla.nesl.sensorsafe.model.User;

public interface UserDatabaseDriver extends DatabaseDriver {
	
	public void changeAdminPassword(String password) throws SQLException;

	public void registerOwner(User newOwner) throws SQLException;
	
}
