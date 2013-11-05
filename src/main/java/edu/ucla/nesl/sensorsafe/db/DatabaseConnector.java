package edu.ucla.nesl.sensorsafe.db;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;

import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabaseDataSourceLoginDriver;

public class DatabaseConnector {
	public static StreamDatabaseDriver getStreamDatabase() throws ClassNotFoundException, SQLException, IOException, NamingException {
		return new InformixStreamDatabaseDriver();
	}
	
	public static UserDatabaseDriver getUserDatabase() throws ClassNotFoundException, SQLException, IOException, NamingException {
		return new InformixUserDatabaseDataSourceLoginDriver();
	}
}
