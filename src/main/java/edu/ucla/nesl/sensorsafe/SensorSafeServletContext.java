package edu.ucla.nesl.sensorsafe;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.wordnik.swagger.config.ConfigFactory;
import com.wordnik.swagger.config.SwaggerConfig;

import edu.ucla.nesl.sensorsafe.tools.Log;

public class SensorSafeServletContext implements ServletContextListener {

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Log.info("SensorSafe is starting up...");
		
		SwaggerConfig config = new SwaggerConfig();
		config.setBasePath("https://128.97.93.251:9443/api");
		ConfigFactory.setConfig(config);
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		Log.info("SensorSafe is being closed...");
	}
}
