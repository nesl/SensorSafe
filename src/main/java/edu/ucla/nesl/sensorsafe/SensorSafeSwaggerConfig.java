package edu.ucla.nesl.sensorsafe;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.wordnik.swagger.config.ConfigFactory;
import com.wordnik.swagger.config.ScannerFactory;
import com.wordnik.swagger.config.SwaggerConfig;
import com.wordnik.swagger.jaxrs.config.DefaultJaxrsScanner;
import com.wordnik.swagger.jersey.JerseyApiReader;
import com.wordnik.swagger.reader.ClassReaders;

public class SensorSafeSwaggerConfig extends HttpServlet {
	
	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		super.init(servletConfig);

		SwaggerConfig config = new SwaggerConfig();
		ConfigFactory.setConfig(config);
		ScannerFactory.setScanner(new DefaultJaxrsScanner());
		ClassReaders.setReader(new JerseyApiReader());
	}
	
	public static void setBasePath(String basePath) {
		SwaggerConfig config = ConfigFactory.config();
		config.setBasePath(basePath);
		ConfigFactory.setConfig(config);
	}
}
