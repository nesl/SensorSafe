package edu.ucla.nesl.sensorsafe;

import java.sql.SQLException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	private static StreamDatabaseDriver streamDb;
	private static UserDatabaseDriver userDb;
	
	public static StreamDatabaseDriver getStreamDatabase(String username) throws ClassNotFoundException, SQLException {
		if (streamDb != null)
			streamDb.setCurrentUser(username);
		return streamDb;
	}

	public static StreamDatabaseDriver getStreamDatabase() {
		return streamDb;
	}
	
	public static UserDatabaseDriver getUserDatabase() {
		return userDb;
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		try {
			if (streamDb != null) 
				streamDb.close();
			if (userDb != null)
				userDb.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		try {
			streamDb = InformixStreamDatabaseDriver.getInstance();
			userDb = InformixUserDatabaseDriver.getInstance();
			streamDb.connect();
			userDb.connect();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
}
