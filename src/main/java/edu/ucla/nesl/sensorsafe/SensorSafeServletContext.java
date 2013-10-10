package edu.ucla.nesl.sensorsafe;

import java.sql.SQLException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.informix.InformixDatabaseDriver;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	private static StreamDatabaseDriver streamDb;
	private static UserDatabaseDriver userDb;
	
	public static StreamDatabaseDriver getStreamDatabase(String username) throws ClassNotFoundException, SQLException {
		if (streamDb != null)
			streamDb.selectDatabase(username);
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
		Log.info("SensorSafe closing down..");
		try {
			streamDb.close();
			userDb.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe starting up..");
		try {
			streamDb = InformixDatabaseDriver.INSTANCE;
			userDb = InformixDatabaseDriver.INSTANCE;
			streamDb.connect();
			userDb.connect();
			userDb.prepareAdminDatabase();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
}
