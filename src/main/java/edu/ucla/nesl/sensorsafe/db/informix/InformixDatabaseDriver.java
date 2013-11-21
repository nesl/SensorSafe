package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.informix.jdbcx.IfxConnectionPoolDataSource;
import com.informix.jdbcx.IfxDataSource;

import edu.ucla.nesl.sensorsafe.db.DatabaseDriver;

abstract public class InformixDatabaseDriver implements DatabaseDriver {

	private static final String INFORMIX_PROP_FILENAME = "etc/informix.prop";
	private static final String INFORMIX_DS_NAME = "SensorsafePooledDataSource";

	protected static DataSource dataSource;

	protected Connection conn;	

	static {
		initDataSourceUsingJndi();
		//initDataSource();
	}

	private static void initDataSourceUsingJndi() {
		try {
			// Using external JNDI service.  
			// In Jetty: DataSource should be configured at jetty.xml and WEB-INF/web.xml
			InitialContext ic = new InitialContext();
			dataSource = (DataSource) ic.lookup("java:comp/env/jdbc/informix-ds");
		} catch (NamingException e) {
			e.printStackTrace();
		}
	}

	private static void initDataSource() {
		try {
			// Set up connection pool without help from external JNDI service.
			new File("./tmp").mkdirs();
			System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
			System.setProperty(Context.PROVIDER_URL, "file:./tmp");
			Context registry;
			registry = new InitialContext();
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
			dataSource = (DataSource) registry.lookup(INFORMIX_DS_NAME);
		} catch (NamingException | SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public InformixDatabaseDriver() throws SQLException, IOException, NamingException, ClassNotFoundException {
		connect();
	}

	@Override
	public void connect() throws SQLException, ClassNotFoundException, IOException, NamingException {
		if (dataSource == null) {
			throw new IOException("DataSource is not initialized.");
		}
		if (conn == null) {
			conn = dataSource.getConnection();
		} else {
			conn.close();
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