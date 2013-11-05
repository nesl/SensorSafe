package edu.ucla.nesl.sensorsafe;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.wordnik.swagger.config.ConfigFactory;
import com.wordnik.swagger.config.SwaggerConfig;
import com.wordnik.swagger.jersey.config.JerseyJaxrsConfig;

import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe is starting up...");
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		Log.info("SensorSafe is being closed...");
	}
}
