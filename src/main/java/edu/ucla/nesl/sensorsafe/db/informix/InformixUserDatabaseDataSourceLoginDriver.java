package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;

import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.User;

public class InformixUserDatabaseDataSourceLoginDriver extends InformixDatabaseDriver implements UserDatabaseDriver {

	public InformixUserDatabaseDataSourceLoginDriver() throws SQLException,
			IOException, NamingException, ClassNotFoundException {
		super();
	}

	private static final String USERS_TABLE = "users";
	private static final String USERNAME_FIELD = "username";
	private static final String PASSWORD_FIELD = "password";
	private static final String ROLE_FIELD = "role";
	
	private static final String ADMIN_ROLE = "admin";
	private static final String OWNER_ROLE = "owner";
	private static final String USER_ROLE = "user";
	
	private static final String DEFAULT_ADMIN_USERNAME = "admin";
	private static final String DEFAULT_ADMIN_PASSWORD = "sensorsafe!";
	private static final String DEFAULT_ADMIN_ROLE_NAME = ADMIN_ROLE;
	
	static {
		try {
			initializeDatabase();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
			pstmt = conn.prepareStatement("CREATE TABLE " + USERS_TABLE + " ("
					+ USERNAME_FIELD + " VARCHAR(100) NOT NULL UNIQUE CONSTRAINT username, "
					+ PASSWORD_FIELD + " VARCHAR(50) NOT NULL,"
					+ ROLE_FIELD + " VARCHAR(100) NOT NULL);");
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
	public void registerOwner(User newOwner) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			try {
				pstmt = conn.prepareStatement("INSERT INTO " + USERS_TABLE 
						+ " (" + USERNAME_FIELD + "," + PASSWORD_FIELD + "," + ROLE_FIELD + ") VALUES (?,?,?)");
				pstmt.setString(1, newOwner.email);
				pstmt.setString(2, newOwner.password);
				pstmt.setString(3, OWNER_ROLE);
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
	public void registerUser(User newUser) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			try {
				pstmt = conn.prepareStatement("INSERT INTO " + USERS_TABLE 
						+ " (" + USERNAME_FIELD + "," + PASSWORD_FIELD + "," + ROLE_FIELD + ") VALUES (?,?,?)");
				pstmt.setString(1, newUser.email);
				pstmt.setString(2, newUser.password);
				pstmt.setString(3, USER_ROLE);
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
	public List<User> getUsers() throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT * FROM " + USERS_TABLE + " WHERE " + ROLE_FIELD + " = ?");
			pstmt.setString(1, USER_ROLE);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				String username = rset.getString(1);
				//String password = rset.getString(2);
				String role = rset.getString(3);
				users.add(new User(username, role, null, null, null));
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
			pstmt = conn.prepareStatement("SELECT * FROM " + USERS_TABLE + " WHERE " + ROLE_FIELD + " = ?");
			pstmt.setString(1, OWNER_ROLE);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				String username = rset.getString(1);
				//String password = rset.getString(2);
				String role = rset.getString(3);
				users.add(new User(username, role, null, null, null));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}
	
	@Override
	public List<User> getAdmins() throws SQLException {
		PreparedStatement pstmt = null;
		List<User> users = new ArrayList<User>();
		try {
			pstmt = conn.prepareStatement("SELECT * FROM " + USERS_TABLE + " WHERE " + ROLE_FIELD + " = ?");
			pstmt.setString(1, ADMIN_ROLE);
			ResultSet rset = pstmt.executeQuery();			
			while (rset.next()) {
				String username = rset.getString(1);
				//String password = rset.getString(2);
				String role = rset.getString(3);
				users.add(new User(username, role, null, null, null));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return users;
	}


}
