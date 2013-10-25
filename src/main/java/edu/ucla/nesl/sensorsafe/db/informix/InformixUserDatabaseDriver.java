package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.NamingException;

import edu.ucla.nesl.sensorsafe.Const;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.User;

public class InformixUserDatabaseDriver extends InformixDatabaseDriver implements UserDatabaseDriver {

	public InformixUserDatabaseDriver() throws SQLException, IOException,
			NamingException, ClassNotFoundException {
		super();
	}

	@Override
	protected void initializeDatabase() throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("SELECT 1 FROM systables WHERE tabname=?");
			pstmt.setString(1, Const.TABLE_ADMIN_USERS);
			ResultSet rset = pstmt.executeQuery();
			boolean isUsersExists = rset.next();
			pstmt.setString(1, Const.TABLE_ADMIN_USER_ROLES);
			rset = pstmt.executeQuery();
			boolean isUserRolesExists = rset.next();
			pstmt.setString(1, Const.TABLE_ADMIN_ROLES);
			rset = pstmt.executeQuery();
			boolean isRolesExists = rset.next();
			if (!isUsersExists || !isUserRolesExists || !isRolesExists) {
				initializeTables();
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	private void initializeTables() throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DROP TABLE IF EXISTS " + Const.TABLE_ADMIN_USERS + ";");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("CREATE TABLE " + Const.TABLE_ADMIN_USERS + " ("
					+ "id SERIAL PRIMARY KEY, "
					+ "username VARCHAR(100) NOT NULL UNIQUE CONSTRAINT username, "
					+ "pwd VARCHAR(50) NOT NULL);");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("DROP TABLE IF EXISTS " + Const.TABLE_ADMIN_USER_ROLES + ";");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("CREATE TABLE " + Const.TABLE_ADMIN_USER_ROLES + " ("
					+ "user_id INTEGER NOT NULL, "
					+ "role_id INTEGER NOT NULL, "
					+ "UNIQUE (user_id, role_id) CONSTRAINT user_role);");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("CREATE INDEX user_id_index ON user_roles (user_id);");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("DROP TABLE IF EXISTS " + Const.TABLE_ADMIN_ROLES + ";");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("CREATE TABLE " + Const.TABLE_ADMIN_ROLES + " ( "
					+ "id SERIAL PRIMARY KEY, "
					+ "role VARCHAR(100) NOT NULL UNIQUE CONSTRAINT role);");
			pstmt.execute(); pstmt.close();
			pstmt = conn.prepareStatement("INSERT INTO " + Const.TABLE_ADMIN_USERS + "(username, pwd) VALUES (?, ?);");
			pstmt.setString(1, Const.ADMIN_USER_NAME);
			pstmt.setString(2, Const.ADMIN_DEFAULT_PASSWORD);
			pstmt.executeUpdate(); pstmt.close();
			pstmt = conn.prepareStatement("INSERT INTO " + Const.TABLE_ADMIN_USER_ROLES + " VALUES (1, 1);");
			pstmt.executeUpdate(); pstmt.close();
			pstmt = conn.prepareStatement("INSERT INTO " + Const.TABLE_ADMIN_ROLES + "(role) VALUES (?);");
			pstmt.setString(1, Const.ADMIN_GROUP_NAME);
			pstmt.executeUpdate(); pstmt.close();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}

	@Override
	public void changeAdminPassword(String password) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("UPDATE " + Const.TABLE_ADMIN_USERS + " SET pwd = ? WHERE username = ?");
			pstmt.setString(1, password);
			pstmt.setString(2, Const.ADMIN_USER_NAME);
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
				pstmt = conn.prepareStatement("INSERT INTO users (username, pwd) VALUES (?,?)");
				pstmt.setString(1, newOwner.email);
				pstmt.setString(2, newOwner.password);
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
			pstmt = conn.prepareStatement("SELECT id FROM roles WHERE role=?");
			pstmt.setString(1, "owner");
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next()) {
				pstmt.close();
				pstmt = conn.prepareStatement("INSERT INTO roles (role) VALUES (?)");
				pstmt.setString(1, "owner");
				pstmt.executeUpdate();
				pstmt.close();
				
				pstmt = conn.prepareStatement("SELECT id FROM roles WHERE role=?");
				pstmt.setString(1, "owner");
				rset = pstmt.executeQuery();
				rset.next();
			}
			int roleId = rset.getInt(1);
			pstmt.close();
			
			pstmt = conn.prepareStatement("SELECT id FROM users WHERE username = ?");
			pstmt.setString(1, newOwner.email);
			rset = pstmt.executeQuery();
			rset.next();
			int userId = rset.getInt(1);
			pstmt.close();

			pstmt = conn.prepareStatement("INSERT INTO user_roles VALUES (?, ?)");
			pstmt.setInt(1, userId);
			pstmt.setInt(2, roleId);
			pstmt.executeUpdate();
			pstmt.close();
			
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}
}
