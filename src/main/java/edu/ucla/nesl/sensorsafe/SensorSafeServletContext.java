package edu.ucla.nesl.sensorsafe;

import java.sql.SQLException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabaseDataSourceLoginDriver;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe is starting up...");
		try {
			InformixStreamDatabaseDriver.initializeDatabase();
			InformixUserDatabaseDataSourceLoginDriver.initializeDatabase();
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		Log.info("SensorSafe is being closed...");
	}
}
