package edu.ucla.nesl.sensorsafe.init;

import java.sql.SQLException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabase;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabase;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe is starting up...");
		try {
			InformixStreamDatabase.initializeDatabase();
			InformixUserDatabase.initializeDatabase();
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		Log.info("SensorSafe is being closed...");
	}
}
