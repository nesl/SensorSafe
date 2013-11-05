package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import edu.ucla.nesl.sensorsafe.db.DatabaseDriver;

abstract public class InformixDatabaseDriver implements DatabaseDriver {

	//private static final String INFORMIX_PROP_FILENAME = "/opt/jetty/etc/informix-sensoract.prop";
	//private static final String INFORMIX_DS_NAME = "SensorsafePooledDataSource";
	
	protected static DataSource dataSource;

	protected Connection conn;	

	public InformixDatabaseDriver() throws SQLException, IOException, NamingException, ClassNotFoundException {
		connect();
	}
	
	public static void initializeConnectionPool() throws SQLException, IOException, NamingException {
		if (dataSource == null) {
		
			// DataSource should be configured at jetty.xml and WEB-INF/web.xml
			InitialContext ic = new InitialContext();
			dataSource = (DataSource) ic.lookup("java:comp/env/jdbc/informix-ds");
			
			/*System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
			System.setProperty(Context.PROVIDER_URL, "file:/tmp");
			Context registry = new InitialContext();
			IfxDataSource ds = new IfxDataSource();
			FileInputStream dsPropFile = new FileInputStream(INFORMIX_PROP_FILENAME);
			ds.readProperties(dsPropFile);
			String CPDSName = ds.getDataSourceName();
			if (CPDSName != null) {
				IfxConnectionPoolDataSource cpds = new IfxConnectionPoolDataSource();
				FileInputStream cpdsPropFile = new FileInputStream(INFORMIX_PROP_FILENAME);
				cpds.readProperties(cpdsPropFile);
				registry.rebind(CPDSName, cpds);		
			}
			registry.rebind(INFORMIX_DS_NAME, ds);
			dataSource = (DataSource) registry.lookup(INFORMIX_DS_NAME);*/
		}
	}
	
	@Override
	public void connect() throws SQLException, ClassNotFoundException, IOException, NamingException {
		if (dataSource == null) {
			initializeConnectionPool();
		}
		if (conn == null) {
			conn = dataSource.getConnection();
		}
	}

	@Override
	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}
}