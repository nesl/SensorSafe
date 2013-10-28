package edu.ucla.nesl.sensorsafe;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import edu.ucla.nesl.sensorsafe.db.informix.InformixDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe is starting up...");
		try {
			InformixDatabaseDriver.initializeConnectionPool();
			InformixUserDatabaseDriver.initializeDatabase();
			InformixStreamDatabaseDriver.initializeDatabase();
		} catch (SQLException | IOException | NamingException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		Log.info("SensorSafe is being closed...");
	}
}
