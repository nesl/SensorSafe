package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.naming.NamingException;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.User;

public class InformixUserDatabase extends InformixDatabaseDriver implements UserDatabaseDriver {

	public InformixUserDatabase() throws SQLException,
	IOException, NamingException, ClassNotFoundException {
		super();
	}

	private static final String USERS_TABLE = "users";

	private static final String USERNAME_FIELD = "username";
	private static final String PASSWORD_FIELD = "password";
	private static final String ROLE_FIELD = "role";
	private static final String OWNER_FIELD = "owner";
	private static final String APIKEY_FIELD = "apikey";
	private static final String EMAIL_FIELD = "email";
	private static final String OAUTH_CONSUMER_KEY_FIELD = "oauth_consumer_key";
	private static final String OAUTH_CONSUMER_SECRET_FIELD = "oauth_consumer_secret";
	private static final String OAUTH_ACCESS_KEY_FIELD = "oauth_access_key";
	private static final String OAUTH_ACCESS_SECRET_FIELD = "oauth_access_secret";

	private static final String DEFAULT_ADMIN_USERNAME = "admin";
	private static final String DEFAULT_ADMIN_PASSWORD = "sensorsafe!";
	private static final String DEFAULT_ADMIN_ROLE_NAME = Roles.ADMIN;

	private static final String USER_TABLE_SCHEMA = USERNAME_FIELD + " VARCHAR(100) NOT NULL, "
													+ PASSWORD_FIELD + " VARCHAR(50) NOT NULL, "
													+ ROLE_FIELD + " VARCHAR(100) NOT NULL, "
													+ OWNER_FIELD + " VARCHAR(100), "
													+ APIKEY_FIELD + " VARCHAR(32), "
													+ OAUTH_CONSUMER_KEY_FIELD + " VARCHAR(32), "
													+ OAUTH_CONSUMER_SECRET_FIELD + " VARCHAR(32), "
													+ OAUTH_ACCESS_KEY_FIELD + " VARCHAR(32), "
													+ OAUTH_ACCESS_SECRET_FIELD + " VARCHAR(32), "
													+ EMAIL_FIELD + " VARCHAR(100), "
													+ "UNIQUE (" + USERNAME_FIELD + "," + OWNER_FIELD + ") CONSTRAINT username_owner";

	private static final String SQL_PROJECTOR = USERNAME_FIELD + ", "
												+ ROLE_FIELD + ", "
												+ OWNER_FIELD + ", "
												+ APIKEY_FIELD + ", "
												+ EMAIL_FIELD + ", "
												+ OAUTH_CONSUMER_KEY_FIELD + ", "
												+ OAUTH_CONSUMER_SECRET_FIELD + ", "
												+ OAUTH_ACCESS_KEY_FIELD + ", "
												+ OAUTH_ACCESS_SECRET_FIELD;

	
	public void clean() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("DROP TABLE IF EXISTS " + USERS_TABLE);
		} finally {
			if (stmt != null)
				stmt.close();
		}
		initializeDatabase();
	}

	public static void initializeDatabase() throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement("SELECT 1 FROM systables WHERE tabname=?");
			pstmt.setString(1, USERS_TABLE);
			ResultSet rset = pstmt.executeQuery();
			boolean isUsersExists = rset.next();
			if (!isUsersExists) {
				initializeUsersTable();
			} 
		} finally { 
			if (pstmt != null)
				pstmt.close();
			if (conn != null)
				conn.close();
		}
	}

	private static void initializeUsersTable() throws SQLException {
		PreparedStatement pstmt = null;
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			pstmt = conn.prepareStatement("DROP TABLE IF EXISTS " + USERS_TABLE + ";");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("CREATE TABLE " + USERS_TABLE + " (" + USER_TABLE_SCHEMA + ");");
			pstmt.execute(); 
			pstmt.close();
			pstmt = conn.prepareStatement("INSERT INTO " + USERS_TABLE 
					+ "(" + USERNAME_FIELD + "," + PASSWORD_FIELD + "," + ROLE_FIELD + ") VALUES (?,?,?);");
			pstmt.setString(1, DEFAULT_ADMIN_USERNAME);
			pstmt.setString(2, DEFAULT_ADMIN_PASSWORD);
			pstmt.setString(3, DEFAULT_ADMIN_ROLE_NAME);
			pstmt.executeUpdate(); 
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (conn != null)
				conn.close();
		}
	}

	@Override
	public void changeAdminPassword(String password) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("UPDATE " + USERS_TABLE + " SET " + PASSWORD_FIELD + " = ? WHERE " + USERNAME_FIELD + " = ?");
			pstmt.setString(1, password);
			pstmt.setString(2, DEFAULT_ADMIN_USERNAME);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void addOwner(User newOwner) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			try {
				pstmt = conn.prepareStatement("INSERT INTO " + USERS_TABLE + " (" 
						+ USERNAME_FIELD + ", " 
						+ PASSWORD_FIELD + ", " 
						+ ROLE_FIELD + ", "
						+ APIKEY_FIELD
						+ ") VALUES (?,?,?,?)");
				pstmt.setString(1, newOwner.username);
				pstmt.setString(2, newOwner.password);
				pstmt.setString(3, Roles.OWNER);
				pstmt.setString(4, newUUIDString());
				pstmt.executeUpdate();
				pstmt.close();
			} catch (SQLException e) {
				if (e.toString().contains("Unique constraint") && e.toString().contains("violated."))
					throw new IllegalArgumentException("Username already registered.");
				else
					throw e;
			}
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	@Override
	public User addConsumer(User newConsumer, String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		String consumerKey = newUUIDString();
		String consumerSecret = newUUIDString();
		String apikey = newUUIDString();
		try {
			pstmt = conn.prepareStatement("INSERT INTO " + USERS_TABLE + " (" 
					+ USERNAME_FIELD + "," 
					+ PASSWORD_FIELD + "," 
					+ ROLE_FIELD + ","
					+ OWNER_FIELD + ", "
					+ OAUTH_CONSUMER_KEY_FIELD + ", "
					+ OAUTH_CONSUMER_SECRET_FIELD + ", "
					+ EMAIL_FIELD + ", "
					+ APIKEY_FIELD
					+ ") VALUES (?,?,?,?,?,?,?,?)");
			pstmt.setString(1, newConsumer.username);
			pstmt.setString(2, newConsumer.password);
			pstmt.setString(3, Roles.CONSUMER);
			pstmt.setString(4, ownerName);
			pstmt.setString(5, consumerKey);
			pstmt.setString(6, consumerSecret);
			pstmt.setString(7, newConsumer.email);
			pstmt.setString(8, apikey);
			pstmt.executeUpdate();
			pstmt.close();
		} catch (SQLException e) {
			if (e.toString().contains("Unique constraint") && e.toString().contains("violated."))
				throw new IllegalArgumentException("Username already registered.");
			else
				throw e;
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
		newConsumer.setPassword(null);
		newConsumer.setRole(Roles.CONSUMER);
		newConsumer.setOAuthConsumerKey(consumerKey);
		newConsumer.setOAuthConsumerSecret(consumerSecret);;
		newConsumer.setApikey(apikey);
		return newConsumer;
	}

	@Override
	public User getUser(String username) throws SQLException {
		PreparedStatement pstmt = null;
		User user = null;
		try {
			pstmt = conn.prepareStatement("SELECT " 
					+ PASSWORD_FIELD + ", " 
					+ ROLE_FIELD 
					+ " FROM " + USERS_TABLE + " WHERE " + USERNAME_FIELD + " = ?");
			pstmt.setString(1, username);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				String password = rset.getString(1);
				String role = rset.getString(2);
				user = new User();
				user.setUsername(username);
				user.setPassword(password);
				user.setRole(role);
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return user;
	}

	private User getUserFromResultSet(ResultSet rset) throws SQLException {
		String username = rset.getString(1);
		String role = rset.getString(2);
		String owner = rset.getString(3);
		String apikey = rset.getString(4);
		String email = rset.getString(5);
		String oauthConsumerKey = rset.getString(6);
		String oauthConsumerSecret = rset.getString(7);
		String oauthAccessKey = rset.getString(8);
		String oauthAccessSecret = rset.getString(9);
		
		User user = new User();
		user.setUsername(username);
		user.setRole(role);
		user.setOwner(owner);
		user.setApikey(apikey);
		user.setEmail(email);				
		user.setOAuthConsumerKey(oauthConsumerKey);
		user.setOAuthConsumerSecret(oauthConsumerSecret);
		user.setOAuthAccessKey(oauthAccessKey);
		user.setOAuthAccessSecret(oauthAccessSecret);

		return user;
	}
	
	@Override
	public List<User> getAllConsumers() throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT " 
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " 
						+ ROLE_FIELD + " = ?");
			pstmt.setString(1, Roles.CONSUMER);		
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				users.add(getUserFromResultSet(rset));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}
	
	@Override
	public List<User> getConsumers(String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT " 
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " 
					+ ROLE_FIELD + " = ? AND "
					+ OWNER_FIELD + " = ?");
			pstmt.setString(1, Roles.CONSUMER);
			pstmt.setString(2, ownerName);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				users.add(getUserFromResultSet(rset));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}

	@Override
	public List<User> getOwners() throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT "
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " + ROLE_FIELD + " = ?");
			pstmt.setString(1, Roles.OWNER);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				users.add(getUserFromResultSet(rset));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}
	
	@Override
	public User getAnOwner(String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		User owner = null;
		try {
			pstmt = conn.prepareStatement("SELECT "
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " + USERNAME_FIELD + " = ? AND " + ROLE_FIELD + " = ?");
			pstmt.setString(1, ownerName);
			pstmt.setString(2, Roles.OWNER);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				owner = getUserFromResultSet(rset);
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return owner;
	}


	@Override
	public List<User> getAdmins() throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT "
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " + ROLE_FIELD + " = ?");
			pstmt.setString(1, Roles.ADMIN);
			ResultSet rset = pstmt.executeQuery();			
			while (rset.next()) {
				users.add(getUserFromResultSet(rset));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}

	protected String newUUIDString() {
		String tmp = UUID.randomUUID().toString();
		return tmp.replaceAll("-", "");
	}

	@Override
	public String getConsumerNameByOAuthKey(String consumerKey) throws SQLException {
		PreparedStatement pstmt = null;
		String username = null;
		try {
			pstmt = conn.prepareStatement("SELECT " + USERNAME_FIELD + " FROM " + USERS_TABLE + " WHERE " + OAUTH_CONSUMER_KEY_FIELD + " = ?");
			pstmt.setString(1, consumerKey);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				username = rset.getString(1);
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return username;
	}

	@Override
	public void addAccessToken(String atKey, String atSecret, String consumerKey) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("UPDATE " + USERS_TABLE + " SET " 
					+ OAUTH_ACCESS_KEY_FIELD + " = ?, "
					+ OAUTH_ACCESS_SECRET_FIELD + " = ? "
					+ " WHERE " + OAUTH_CONSUMER_KEY_FIELD + " = ?");
			pstmt.setString(1, atKey);
			pstmt.setString(2, atSecret);
			pstmt.setString(3, consumerKey);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public User getUserByApikey(String apikey) throws SQLException {
		User user = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("SELECT "
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE "
						+ APIKEY_FIELD + " = ?");
			pstmt.setString(1, apikey);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				user = getUserFromResultSet(rset);
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
		return user;
	}

	@Override
	public void deleteOwner(String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM " + USERS_TABLE + " WHERE " 
					+ USERNAME_FIELD + " = ? AND " 
					+ ROLE_FIELD + " = ?");
			pstmt.setString(1, ownerName);
			pstmt.setString(2, Roles.OWNER);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	@Override
	public User deleteConsumer(String consumerName, String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		User deletedUser = null;
		try {
			pstmt = conn.prepareStatement("SELECT "
					+ SQL_PROJECTOR
					+ " FROM " + USERS_TABLE + " WHERE " 
						+ OWNER_FIELD + " = ? AND "
						+ ROLE_FIELD + " = ?");
			pstmt.setString(1, ownerName);
			pstmt.setString(2, Roles.CONSUMER);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				deletedUser = getUserFromResultSet(rset);
			}
			pstmt.close();
			
			pstmt = conn.prepareStatement("DELETE FROM " + USERS_TABLE + " WHERE " 
					+ USERNAME_FIELD + " = ? AND " 
					+ ROLE_FIELD + " = ? AND "
					+ OWNER_FIELD + " = ?");
			pstmt.setString(1, consumerName);
			pstmt.setString(2, Roles.CONSUMER);
			pstmt.setString(3, ownerName);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
		return deletedUser;
	}

}
