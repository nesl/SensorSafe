package edu.ucla.nesl.sensorsafe.db;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;

import edu.ucla.nesl.sensorsafe.db.informix.InformixStreamDatabase;
import edu.ucla.nesl.sensorsafe.db.informix.InformixUserDatabase;

public class DatabaseConnector {
	public static StreamDatabaseDriver getStreamDatabase() throws ClassNotFoundException, SQLException, IOException, NamingException {
		return new InformixStreamDatabase();
	}
	
	public static UserDatabaseDriver getUserDatabase() throws ClassNotFoundException, SQLException, IOException, NamingException {
		return new InformixUserDatabase();
	}
}
