package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;
import java.util.List;

import edu.ucla.nesl.sensorsafe.model.User;

public interface UserDatabaseDriver extends DatabaseDriver {
	
	public void changeAdminPassword(String password) throws SQLException;

	public void registerOwner(User newOwner) throws SQLException;

	public List<User> getUsers() throws SQLException;

	public List<User> getOwners() throws SQLException;

	public List<User> getAdmins() throws SQLException;	
}
