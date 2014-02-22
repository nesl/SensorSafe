package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;
import java.util.List;

import edu.ucla.nesl.sensorsafe.model.User;

public interface UserDatabaseDriver extends DatabaseDriver {
	
	public void changeAdminPassword(String password) throws SQLException;

	public void addOwner(User newOwner) throws SQLException;

	public List<User> getConsumers(String ownerName) throws SQLException;

	public List<User> getOwners() throws SQLException;

	public List<User> getAdmins() throws SQLException;

	public User addConsumer(User newConsumer, String ownerName) throws SQLException;

	public User getUser(String username) throws SQLException;
	
	public String getUserEmail(String username) throws SQLException;
	
	public void clean() throws SQLException;

	public String getConsumerNameByOAuthKey(String consumerKey) throws SQLException;

	public void addAccessToken(String atKey, String atSecret, String key) throws SQLException;

	public User getAnOwner(String ownerName) throws SQLException;

	public User getUserByApikey(String apikey) throws SQLException;

	public void deleteOwner(String ownerName) throws SQLException;

	public User deleteConsumer(String consumerName, String ownerName) throws SQLException;

	public List<User> getAllConsumers() throws SQLException;
}
