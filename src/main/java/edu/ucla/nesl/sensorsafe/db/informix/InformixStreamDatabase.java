package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NamingException;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.Statistics;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.model.TemplateParameter;
import edu.ucla.nesl.sensorsafe.model.TemplateParameterDefinition;
import edu.ucla.nesl.sensorsafe.tools.DiffPrivNoiseGenerator;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class InformixStreamDatabase extends InformixDatabaseDriver implements StreamDatabaseDriver {

	public InformixStreamDatabase() throws SQLException, IOException,
	NamingException, ClassNotFoundException {
		super();
	}

	private static final String ORIGIN_TIMESTAMP = "2000-01-01 00:00:00.00000";

	private static final String BULK_LOAD_DATA_FILE_NAME_PREFIX = "/tmp/bulkload_data_";

	private static final String SQL_DATE_TIME_PATTERN_WITH_FRACTION = "yyyy-MM-dd HH:mm:ss.SSSSS";
	private static final String SQL_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

	private static final long QUERY_DURATION_LIMIT = 7;

	// Informix TimeSeries flags
	private static final int TSOPEN_REDUCED_LOG = 256;

	private PreparedStatement storedPstmt;
	private ResultSet storedResultSet;
	private Stream storedStream;
	private List<String> storedTempTables = new ArrayList<String>();
	private String storedTempViewName;
	private String storedTempRowTypeName;

	private void dropStoredTempRowTypeName() throws SQLException {
		if (storedTempRowTypeName != null) {
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				String sql = "DROP ROW TYPE IF EXISTS " + storedTempRowTypeName + " RESTRICT;";
				Log.info(sql);
				stmt.execute(sql);				
			} finally {
				if (stmt != null) {
					stmt.close();
				}
			}
			storedTempRowTypeName = null;
		}
	}

	private void dropStoredTempTables() throws SQLException {
		if (!storedTempTables.isEmpty()) {
			Statement stmt = null;
			for (String table : storedTempTables) {
				try {
					stmt = conn.createStatement();
					String sql = "DROP TABLE IF EXISTS " + table;
					Log.info(sql);
					stmt.execute(sql);				
				} finally {
					if (stmt != null) {
						stmt.close();
					}
				}
			}
			storedTempTables.clear();
		}
	}

	private void dropStoredTempView() throws SQLException {
		if (storedTempViewName != null) {
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				String sql = "DROP VIEW IF EXISTS " + storedTempViewName;
				Log.info(sql);
				stmt.execute(sql);				
			} finally {
				if (stmt != null) {
					stmt.close();
				}
			}
			storedTempViewName = null;
		}
	}

	@Override
	public void close() throws SQLException {
		dropTemps();
		super.close();
	}

	@Override
	public void clean() throws SQLException, ClassNotFoundException {
		Statement stmt1 = null;
		Statement stmt2 = null;

		try {
			stmt1 = conn.createStatement();
			ResultSet rset = stmt1.executeQuery("SELECT 1 FROM systables WHERE tabname='streams';");
			if (rset.next()) {
				stmt1.close();
				stmt1 = conn.createStatement();
				rset = stmt1.executeQuery("SELECT channels FROM streams;");
				while (rset.next()) {
					stmt2 = conn.createStatement();
					String prefix = Stream.getChannelFormatPrefix(Stream.getListChannelFromSqlArray(rset.getArray(1)));
					stmt2.execute("DROP TABLE IF EXISTS " + prefix + "vtable");
					stmt2.execute("DROP TABLE IF EXISTS " + prefix + "streams");
					stmt2.execute("DROP ROW TYPE IF EXISTS " + prefix + "rowtype RESTRICT");
				}
			}
			stmt1.close();
			stmt1 = conn.createStatement();
			stmt1.execute("DROP TABLE IF EXISTS streams;");
			stmt1.execute("DROP TABLE IF EXISTS rules;");
			stmt1.execute("DELETE FROM CalendarPatterns WHERE cp_name = 'every_sec';");
			stmt1.execute("DELETE FROM CalendarTable WHERE c_name = 'sec_cal';");

			initializeDatabase();
		} finally {
			if (stmt1 != null) 
				stmt1.close();
			if (stmt2 != null) 
				stmt2.close();
		}
	}

	public static void initializeDatabase() throws SQLException, ClassNotFoundException {
		PreparedStatement pstmt = null;
		Statement stmt = null;
		Connection conn = null;
		try {
			// Check if all tables are there in database.
			conn = dataSource.getConnection();

			// Check calendars
			stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery("SELECT cp_name FROM CalendarPatterns WHERE cp_name = 'every_sec';");
			if (!rset.next()) {
				stmt.execute("INSERT INTO CalendarPatterns VALUES('every_sec', '{1 on}, second');");
			}
			rset = stmt.executeQuery("SELECT c_name, c_calendar FROM CalendarTable WHERE c_name = 'sec_cal';");
			if (!rset.next()) {
				stmt.execute("INSERT INTO CalendarTable(c_name, c_calendar) VALUES ('sec_cal', 'startdate(1753-01-01 00:00:00.00000), pattname(every_sec)');");
			}

			stmt.close();

			// Check streams table
			String sql = "SELECT 1 FROM systables WHERE tabname=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "streams");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createStreamsTable(conn);
			}

			// Check rules table
			pstmt.setString(1, "rules");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createRulesTable(conn);
			}

			// Check macros table
			pstmt.setString(1, "macros");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createMacrosTable(conn);
			}

			pstmt.setString(1, "channel_statistics");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createChannelStatisticsTable(conn);
			}
			
			pstmt.close();

			// Add type map information for calendars
			Map<String, Class<?>> typeMap = conn.getTypeMap();
			typeMap.put("calendarpattern", Class.forName("com.informix.timeseries.IfmxCalendarPattern"));
			typeMap.put("calendar", Class.forName("com.informix.timeseries.IfmxCalendar"));

			// Add type map information for row types
			sql = "SELECT channels FROM streams";
			pstmt = conn.prepareStatement(sql);
			rset = pstmt.executeQuery();
			while (rset.next()) {
				String typeName = "timeseries(" + Stream.getRowTypeName(Stream.getListChannelFromSqlArray(rset.getArray(1))) + ")";
				if (!typeMap.containsKey(typeName)) {
					typeMap.put(typeName, Class.forName("com.informix.timeseries.IfmxTimeSeries"));
				}
			}			
			conn.setTypeMap(typeMap);
			
			// check channel statistics table.
			//TODO: checkChannelStatistics(conn);
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (stmt != null) 
				stmt.close();
			if (conn != null)
				conn.close();
		}

	}

	private static void checkChannelStatistics(Connection conn) throws SQLException {
		PreparedStatement pstmt = null;
		
		String createRowTypeSql = "CREATE ROW TYPE IF NOT EXISTS float_rowtype ("
				+ "timestamp DATETIME YEAR TO FRACTION(5), "
				+ "channel1 float)";
		
		try {
			pstmt = conn.prepareStatement(createRowTypeSql);
			pstmt.execute();
		} finally {
			if (pstmt != null) {
				pstmt.close();
				pstmt = null;
			}
		} 
		List<Stream> list = getStreamList(conn, null);
		for (Stream s : list) {
			int i = 1;
			for (Channel c : s.channels){
				if (c.statistics == null) {
					calculateChannelStatistics(conn, s, c, i);
				}
				i++;
			}
		}
	}

	private static void calculateChannelStatistics(Connection conn, Stream s, Channel c, int channelId) throws SQLException {
		
		if (!c.type.equals("float") && !c.type.equals("int")) {
			return;
		}
		
		Log.info("Calculating channel statistics: " + s.name + "." + c.name + "...");
		
		double min, max;

		String aggregateSql = "SELECT AggregateRange('min($channel" + channelId + ")', tuples, 0)::float_rowtype"
				+ " FROM " + s.getStreamTableName() + " WHERE id = ?";
		
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(aggregateSql);
			pstmt.setInt(1, s.id);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				Struct struct = (Struct)rset.getObject(1);
				min = (Double)struct.getAttributes()[1];
			} else {
				throw new IllegalStateException("ResultSet is null.");
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}

		aggregateSql = "SELECT AggregateRange('max($channel" + channelId + ")', tuples, 0)::float_rowtype"
				+ " FROM " + s.getStreamTableName() + " WHERE id = ?";
		
		try {
			pstmt = conn.prepareStatement(aggregateSql);
			pstmt.setInt(1, s.id);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				Struct struct = (Struct)rset.getObject(1);
				max = (Double)struct.getAttributes()[1];
			} else {
				throw new IllegalStateException("ResultSet is null.");
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
				pstmt = null;
			}
		}

		// Check if there is existing statistics
		String sql = "SELECT * FROM channel_statistics WHERE stream_name = ? AND channel_name = ?;";
		boolean isUpdate = false;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, s.name);
			pstmt.setString(2, c.name);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				isUpdate = true;
				sql = "UPDATE channel_statistics SET "
						+ "min_value = ?, "
						+ "max_value = ? "
						+ "WHERE id = ? AND stream_name = ? AND channel_name = ?;";
			} else {
				isUpdate = false;
				sql = "INSERT INTO channel_statistics VALUES (?,?,?,?,?)";		
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
		
		// update/insert statistics
		try {
			pstmt = conn.prepareStatement(sql);
			if (isUpdate) {
				pstmt.setDouble(1, min);
				pstmt.setDouble(2, max);
				pstmt.setInt(3, s.id);
				pstmt.setString(4, s.name);
				pstmt.setString(5, c.name);
			} else {
				pstmt.setInt(1, s.id);
				pstmt.setString(2, s.name);
				pstmt.setString(3, c.name);
				pstmt.setDouble(4, min);
				pstmt.setDouble(5, max);
			}
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	private static void createChannelStatisticsTable(Connection conn) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE channel_statistics ("
					+ "id BIGINT NOT NULL, "
					+ "stream_name VARCHAR(100) NOT NULL, "
					+ "channel_name VARCHAR(100) NOT NULL, "
					+ "min_value FLOAT, "
					+ "max_value FLOAT, "
					+ "UNIQUE (id, stream_name, channel_name) CONSTRAINT unique_channel_stat) LOCK MODE ROW;");
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	private static void createStreamsTable(Connection conn) throws SQLException {

		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			stmt.execute("CREATE TABLE streams ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "name VARCHAR(100) NOT NULL, "
					+ "tags VARCHAR(255), "
					+ "channels LIST(ROW(name VARCHAR(100),type VARCHAR(100)) NOT NULL), "
					+ "UNIQUE (owner, name) CONSTRAINT owner_stream_name) LOCK MODE ROW;");
		} 
		finally {
			if (stmt != null)
				stmt.close();
		}
	}

	private static void createRulesTable(Connection conn) throws SQLException {

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE rules ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "priority BIGINT, "
					+ "target_users SET(VARCHAR(100) NOT NULL), "
					+ "target_streams SET(VARCHAR(100) NOT NULL), "
					+ "condition VARCHAR(255), "
					+ "action VARCHAR(255) NOT NULL, "					
					+ "template_name VARCHAR(255), "
					+ "is_aggregator BOOLEAN NOT NULL"
					+ ") LOCK MODE ROW;");
		} 
		finally {
			if (stmt != null)
				stmt.close();
		}
	}

	private static void createMacrosTable(Connection conn) throws SQLException {

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE macros ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "macro_name VARCHAR(255) NOT NULL, "
					+ "macro_value VARCHAR(255) NOT NULL) LOCK MODE ROW;");
		} 
		finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Override
	public void addUpdateRuleTemplate(String owner, Rule rule) throws SQLException {
		boolean isAggregator = rule.isValidRule();

		String targetUsers = SqlBuilder.getSqlSetString(rule.target_users);
		String targetStreams = SqlBuilder.getSqlSetString(rule.target_streams);

		PreparedStatement pstmt = null;

		// Check if duplicate aggregate rules;
		checkDuplicateAggregateRules(owner, rule);
		
		// Check if there are template rules with same name.
		if (rule.template_name != null) {
			if (rule.priority != Integer.MAX_VALUE) {
				throw new IllegalArgumentException("Template rules cannot have priority.");
			}

			try {
				pstmt = conn.prepareStatement("SELECT * FROM rules WHERE template_name = ?");
				pstmt.setString(1, rule.template_name);
				ResultSet rset = pstmt.executeQuery();
				if (rset.next()) {
					throw new IllegalArgumentException("Template name already registered.");
				}
			} finally {
				if (pstmt != null) {
					pstmt.close();
					pstmt = null;
				}
			}
		}

		// Insert the rule.
		pstmt = null;
		try {
			String sql;
			if (rule.id == 0) {
				// Add new rule
				sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, priority, template_name, is_aggregator) VALUES (?,?,?,?,?,?,?,?)";
			} else {
				// Update existing rule or create new rule if id doesn't exist.
				pstmt = conn.prepareStatement("SELECT 1 FROM rules WHERE id = ?");
				pstmt.setInt(1, rule.id);
				ResultSet rset = pstmt.executeQuery();
				if (rset.next()) {
					sql = "UPDATE rules SET owner = ?, "
							+ "target_users = ?, "
							+ "target_streams = ?, "
							+ "condition = ?, "
							+ "action = ?, "
							+ "priority = ?, "
							+ "template_name = ?, "
							+ "is_aggregator = ? "
							+ "WHERE id = ?";
				} else {
					sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, priority, template_name, is_aggregator, id) VALUES (?,?,?,?,?,?,?,?,?)";
				}
			}
			if (pstmt != null)
				pstmt.close();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, targetUsers);
			pstmt.setString(3, targetStreams);
			pstmt.setString(4, rule.condition);
			pstmt.setString(5, rule.action);
			if (isAggregator) {
				pstmt.setInt(6, 0);
			} else {
				pstmt.setInt(6, rule.priority);
			}
			pstmt.setString(7, rule.template_name);
			pstmt.setBoolean(8, isAggregator);
			if (rule.id != 0) {
				pstmt.setInt(9, rule.id);
			} 
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	
	private void checkDuplicateAggregateRules(String owner, Rule rule) throws SQLException {
		if (!Aggregator.isAggregateExpression(rule.action)) {
			return;
		}
		
		Set<String> users;
		Set<String> streams;
		if (rule.target_users == null) {
			users = new HashSet<String>();
			users.add(null);
		} else {
			users = rule.target_users;
		}
		if (rule.target_streams == null) {
			streams = new HashSet<String>();
			streams.add(null);
		} else {
			streams = rule.target_streams;
		}
		
		for (String user : users) {
			for (String stream : streams) {
				String sql = "SELECT count(*) FROM rules WHERE owner = ? AND is_aggregator = ? ";
				if (user != null) {
					sql += 	"AND ( ? IN target_users OR target_users IS NULL) ";
				}
				if (stream != null) {
					sql += 	"AND ( ? IN target_streams OR target_streams IS NULL) ";
				}
				
				PreparedStatement pstmt = null;
				try {
					pstmt = conn.prepareStatement(sql);
					pstmt.setString(1, owner);
					pstmt.setBoolean(2, true);
					int i = 3;
					if (user != null) {
						pstmt.setString(i, user);
						i++;
					}
					if (stream != null) {
						pstmt.setString(i, stream);
					}
					ResultSet rset = pstmt.executeQuery();
					if (rset.next()) {
						if (rset.getInt(1) > 0) {
							throw new IllegalArgumentException("Duplicate aggregate rule detected.");
						}
					} else {
						assert false;
					}
				} finally {
					if (pstmt != null) {
						pstmt.close();
					}
				}
			}
		}
	}

	@Override
	public List<Rule> getRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		List<Rule> rules = new ArrayList<Rule>();
		try {
			String sql = "SELECT id, target_users, target_streams, condition, action, priority, is_aggregator FROM rules WHERE owner = ? AND template_name IS NULL";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				int id = rset.getInt(1);
				Array sqlArr = rset.getArray(2);
				Object[] targetUsers = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				sqlArr = rset.getArray(3);
				Object[] targetStreams = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				rules.add(new Rule(id, targetUsers, targetStreams, rset.getString(4), rset.getString(5), rset.getInt(6)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return rules;
	}

	@Override
	public void deleteAllRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM rules WHERE owner = ? AND template_name IS NULL");
			pstmt.setString(1, owner);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteRule(String owner, int id) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM rules WHERE owner = ? AND id = ? AND template_name IS NULL");
			pstmt.setString(1, owner);
			pstmt.setInt(2, id);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void createStream(Stream stream) throws SQLException, ClassNotFoundException {

		// Check if tupleFormat is valid
		if (!stream.isChannelsValid())
			throw new IllegalArgumentException("Invalid channel definition.");

		// Check if stream name exists.
		if ( isStreamNameExist(stream.owner, stream.name) )
			throw new IllegalArgumentException("Stream name (" + stream.name + ") already exists.");

		// Create row type if not exists.
		createRowType(stream.channels);

		// Create stream table if not exists.
		createStreamTable(stream);

		// Insert into streams table
		stream = insertIntoStreamTable(stream);

		// Insert into timeseries streams table
		insertIntoTimeseriesTable(stream);

		// Create a virtual table.
		createVirtualTable(stream);
	}

	private boolean isStreamNameExist(String owner, String streamName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "SELECT COUNT(name) FROM streams WHERE owner = ? AND name = ?;";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, streamName);
			ResultSet rSet = pstmt.executeQuery();
			rSet.next();
			if ( rSet.getInt(1) > 0 )
				return true;
		} finally {
			if (pstmt != null)
				pstmt.close();
		}

		return false;
	}

	/*private int getNewStreamId() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rSet = stmt.executeQuery("SELECT MAX(id) FROM streams;");
			rSet.next();
			int maxId = rSet.getInt(1);
			return maxId + 1;
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}*/

	private void createRowType(List<Channel> channels) throws SQLException, ClassNotFoundException {
		String rowTypeName = Stream.getRowTypeName(channels);
		String channelSqlPart = "";

		int channelID = 1;			
		for (Channel channel: channels) {
			if (channel.type.equals("text")) {
				channelSqlPart += "channel" + channelID + " VARCHAR(255), ";
			} else if (channel.type.equals("float") || channel.type.equals("int")) {
				channelSqlPart += "channel" + channelID + " " + channel.type.toUpperCase() + ", ";
			}
			channelID += 1;
		}
		
		channelSqlPart = channelSqlPart.substring(0, channelSqlPart.length() - 2);
		
		String sql = "CREATE ROW TYPE IF NOT EXISTS " + rowTypeName + "("
				+ "timestamp DATETIME YEAR TO FRACTION(5), "
				+ channelSqlPart + ")";
		
		Log.info(sql);
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} finally {
			if (stmt != null) 
				stmt.close();
		}
		
		// Add the type map information
		Map<String, Class<?>> typeMap = conn.getTypeMap();
		String typeName = "timeseries(" + Stream.getChannelFormatPrefix(channels)+ "rowtype)";
		if (!typeMap.containsKey(typeName)) {
			typeMap.put(typeName, Class.forName("com.informix.timeseries.IfmxTimeSeries"));
			conn.setTypeMap(typeMap);
		}
	}	

	private void createStreamTable(Stream stream) throws SQLException {
		String rowtype = stream.getRowTypeName();
		String table = stream.getStreamTableName();
		String sql = "CREATE TABLE IF NOT EXISTS " + table 
				+ " (id BIGINT NOT NULL PRIMARY KEY, tuples TIMESERIES(" + rowtype + ")) LOCK MODE ROW";
		
		Log.info(sql);
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} finally {
			if (stmt != null) 
				stmt.close();
		}
	}

	private Stream insertIntoStreamTable(Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("INSERT INTO streams (owner, name, tags, channels) VALUES (?,?,?,?)");
			pstmt.setString(1, stream.owner);
			pstmt.setString(2, stream.name);
			pstmt.setString(3, stream.tags);
			pstmt.setString(4, stream.getChannelsAsSqlList());
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = conn.prepareStatement("SELECT id FROM streams WHERE owner=? AND name=?");
			pstmt.setString(1, stream.owner);
			pstmt.setString(2, stream.name);
			ResultSet rset = pstmt.executeQuery();
			
			if (!rset.next()) {
				throw new IllegalStateException("rset.next() is null");
			}

			stream.setStreamId(rset.getInt(1));
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
		return stream;
	}

	private void insertIntoTimeseriesTable(Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String table = stream.getStreamTableName();
			String sql = "INSERT INTO " + table + " VALUES (?, "
					+ "'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, stream.id);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	private void createVirtualTable(Stream stream) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			String prefix = stream.getChannelFormatPrefix();
			String vtableName = prefix + "vtable";

			pstmt = conn.prepareStatement("SELECT 1 FROM systables WHERE tabname=?");
			pstmt.setString(1, vtableName);
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next()) {
				pstmt.close();
				String stableName = prefix + "streams";
				pstmt = conn.prepareStatement("EXECUTE PROCEDURE TSCreateVirtualTAB(?, ?)");
				pstmt.setString(1, vtableName);
				pstmt.setString(2, stableName);
				pstmt.execute();
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void addTuple(String owner, String streamName, String strTuple) throws SQLException {
		// Check if stream name exists.
		if ( !isStreamNameExist(owner, streamName) )
			throw new IllegalArgumentException("Stream name (" + streamName + ") does not exists.");

		Stream stream = getStream(owner, streamName);
		TimestampValues tsValues = parseStrTuple(strTuple, stream.getChannelFormatPrefix());
		
		putTimeseriesElement(stream, tsValues);
		updateChannelStatistics(stream, tsValues);
	}
	
	private void updateChannelStatistics(Stream stream, TimestampValues tsValues) throws SQLException {
		int i = 1;
		String format[] = stream.getChannelFormatPrefix().split("_");
		for (Channel c : stream.channels){
			if (isNumeric(c)) {
				if (c.statistics == null) {
					//calculateChannelStatistics(conn, stream, c, i);
					throw new IllegalStateException("Channel statistics is null.");
				} else {
					double min = c.statistics.min;
					double max = c.statistics.max;
					double value;
					Object valueObj = tsValues.values.get(i-1);
					if (format[i-1].equals("float")) {
						if (valueObj instanceof Double) {
							value = (Double)valueObj;
						} else if (valueObj instanceof Integer) {
							value = (Integer)valueObj;
						} else {
							throw new IllegalStateException("Can't determine value type.");
						}
					} else if (format[i-1].equals("int")) {
						value = (Integer)valueObj;
					} else {
						throw new IllegalStateException("Format is not numeric.");
					}
					if (min > value) {
						updateChannelStatisticsMin(stream.name, c.name, value);
					}
					if (max < value) {
						updateChannelStatisticsMax(stream.name, c.name, value);
					}
				}
			}
			i++;
		}
	}

	private void updateChannelStatisticsMax(String streamName, String channelName, double value) throws SQLException {
		String updateSql = "UPDATE channel_statistics SET "
				+ "max_value = ? "
				+ "WHERE stream_name = ? AND channel_name = ?;";
		
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(updateSql);
			pstmt.setDouble(1, value);
			pstmt.setString(2, streamName);
			pstmt.setString(3, channelName);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	private void updateChannelStatisticsMin(String streamName, String channelName, double value) throws SQLException {
		String updateSql = "UPDATE channel_statistics SET "
				+ "min_value = ? "
				+ "WHERE stream_name = ? AND channel_name = ?;";
		
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(updateSql);
			pstmt.setDouble(1, value);
			pstmt.setString(2, streamName);
			pstmt.setString(3, channelName);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	private boolean isNumeric(Channel c) {
		if (c.type.equals("float") || c.type.equals("int")) {
			return true;
		} else {
			return false;
		}
	}

	private void putTimeseriesElement(Stream stream, TimestampValues tsValues) throws SQLException {

		String format[] = stream.getChannelFormatPrefix().split("_");

		if (format.length != tsValues.values.size()) {
			throwInvalidTupleFormat(format);
		}

		String sql = "UPDATE " + stream.getStreamTableName() + " "
				+ "SET tuples = PutElem(tuples, "
				+ "row(?,";
		for (int i = 0; i < tsValues.values.size(); i++) {
			sql += "?,";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")::" + stream.getRowTypeName() +") WHERE id = ?";

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, tsValues.timestamp);
			int i;
			for (i = 0; i < tsValues.values.size(); i++) {
				Object value = tsValues.values.get(i);
				if (format[i].equals("int")) {
					pstmt.setInt(i+2, (Integer)value);
				} else if (format[i].equals("float")) {
					if (value instanceof Double ) {
						pstmt.setDouble(i+2, (Double)value);
					} else if (value instanceof Integer) {
						pstmt.setInt(i+2, (Integer)value);
					} else {
						throwInvalidTupleFormat(format);
					}
				} else if (format[i].equals("text")) {
					pstmt.setString(i+2, (String)value);
				} else {
					throwInvalidTupleFormat(format);
				}
			}
			pstmt.setInt(i + 2, stream.id);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			if (e.toString().contains("Extra characters at the end of a datetime or interval."))
				throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_TIMESTAMP_FORMAT);
			else if (e.toString().contains("No cast from ROW")) 
				throwInvalidTupleFormat(format);
			else 
				throw e;
		} catch (ClassCastException e) {
			throwInvalidTupleFormat(format);
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private TimestampValues parseStrTuple(String strTuple, String prefix) {
		Object objTuple = JSONValue.parse(strTuple);
		List<Object> values = new ArrayList<Object>();
		Timestamp timestamp = null;

		if (objTuple instanceof JSONArray) {
			boolean isFirst = true;

			for (Object obj: (JSONArray)objTuple) {
				if (isFirst) {
					if (obj == null)
						timestamp = new Timestamp(System.currentTimeMillis());
					else {
						timestamp = parseTimestamp(obj);
					}
					isFirst = false;
				} else {
					values.add(obj);
				}
			}
		} else if (objTuple instanceof JSONObject) {
			JSONObject json = (JSONObject)objTuple;
			Object objTimestamp = json.get("timestamp");
			if (objTimestamp == null) {
				timestamp = new Timestamp(System.currentTimeMillis());
			} else {
				timestamp = parseTimestamp(objTimestamp);
			}
			JSONArray tuples = (JSONArray)json.get("tuple");

			for (Object obj: tuples) {
				values.add(obj);
			}
		} else {
			throwInvalidTupleFormat(prefix.split("_"));
		}

		return new TimestampValues(timestamp, values);
	}

	private Timestamp parseTimestamp(Object obj) {
		Timestamp timestamp = null;
		try {
			timestamp = Timestamp.valueOf((String)obj);
		} catch (IllegalArgumentException | ClassCastException e) {
			throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_TIMESTAMP_FORMAT);
		}
		return timestamp;
	}

	private void throwInvalidTupleFormat(String[] format) {
		String tupleFormat = "[ " + ExceptionMessages.VALID_TIMESTAMP_FORMAT + " (or null), ";
		for (String strFormat: format) {
			tupleFormat += strFormat + ", ";
		}
		tupleFormat = tupleFormat.substring(0, tupleFormat.length() - 2) + " ]";
		throw new IllegalArgumentException("Invalid tuple data type. Expected tuple format: " + tupleFormat);
	}

	private class TimestampValues {
		public Timestamp timestamp;
		public List<Object> values;

		public TimestampValues(Timestamp timestamp, List<Object> values) {
			this.timestamp = timestamp;
			this.values = values;
		}
	}

	protected String newUUIDString() {
		String tmp = UUID.randomUUID().toString();
		return tmp.replaceAll("-", "");
	}

	@Override
	public boolean prepareQuery(
			String requestingUser,
			String streamOwner, 
			String streamName, 
			String startTime, 
			String endTime, 
			String aggregator,
			String filter,
			int limit, 
			int offset,
			boolean isUpdateNumSamples) throws SQLException, ClassNotFoundException {

		// Check if stream name exists.
		if ( !isStreamNameExist(streamOwner, streamName) )
			throw new IllegalArgumentException("Stream (" + streamName + ") of owner (" + streamOwner + ") does not exists.");

		// Check valid timestamps
		Timestamp startTs = null, endTs = null;
		DateTime startDateTime = null, endDateTime = null;
		if (startTime != null) {
			startDateTime = getDateTimeFromString(startTime);
			startTs = new Timestamp(startDateTime.getMillis());
		}
		if (endTime != null) {
			endDateTime = getDateTimeFromString(endTime);	
			endTs = new Timestamp(endDateTime.getMillis());
		}
		checkStartBeforeEndTime(startDateTime, endDateTime);

		// Get some basic information.
		Stream stream = getStream(streamOwner, streamName);

		// Build SQL
		SqlBuilder sql = new SqlBuilder(offset, limit, startTs, endTs, filter, stream);

		// Apply rule condition.
		if (!streamOwner.equals(requestingUser)) {
			String ruleAggregator = getAggregateRuleExpression(streamOwner, requestingUser, stream);
			String ruleCond = getRuleCondition(streamOwner, requestingUser, stream);
			
			if (ruleCond == null && ruleAggregator == null) {
				return false;
			} else if (ruleCond != null && ruleCond.equals("allow all")) {
				ruleCond = null;
			}
			sql.condRules = ruleCond;

			sql.makeValidSql(getMacros(streamOwner));
			
			if (ruleAggregator != null) {
				Aggregator agg = new Aggregator(ruleAggregator, stream.channels);
				
				String tempCondFilter = sql.condFilter;
				sql.condFilter = null;
				
				sql = processConditionOnOtherStreams(sql, streamOwner);
				if (sql.condRules != null) {
					sql = processFilterForAggregate(agg, sql);
				}
				sql = processAggregate(agg, sql);
				
				sql.condFilter = tempCondFilter;
				sql.condRules = null;
			}
		} else {
			sql.makeValidSql(getMacros(streamOwner));
		}
		
		sql = processConditionOnOtherStreams(sql, streamOwner);

		if (aggregator != null) {
			Aggregator agg = new Aggregator(aggregator, stream.channels);
			
			// If aggregator with no filter, no rules, we can skip creating temporary filtered result.
			if (sql.condFilter != null || sql.condRules != null) {
				sql = processFilterForAggregate(agg, sql);
			}
			sql = processAggregate(agg, sql);
		}
		
		executeQuery(sql, stream, isUpdateNumSamples);
		
		return true;
	}

	private String getAggregateRuleExpression(String streamOwner, String requestingUser, Stream stream) throws SQLException {

		String sql = "SELECT action "
				+ "FROM rules "
				+ "WHERE owner = ? "
				+ "AND is_aggregator = ?"
				+ "AND ( ? IN target_streams OR target_streams IS NULL) "
				+ "AND ( ? IN target_users OR target_users IS NULL ) "
				+ "AND template_name IS NULL "
				+ "ORDER BY priority DESC;";

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, streamOwner);
			pstmt.setBoolean(2, true);
			pstmt.setString(3, stream.name);
			pstmt.setString(4, requestingUser);

			ResultSet rset = pstmt.executeQuery();
			
			if (rset.next()) {
				return rset.getString(1);
			} else {
				return null;
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	private void executeQuery(SqlBuilder sql, Stream stream, boolean isUpdateNumSamples) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			// Get number of samples.
			if (isUpdateNumSamples) {
				long n = executeGetCountSql(sql.streamTableName, stream.id, sql.startTime, sql.endTime);
				stream.num_samples = n;
			}
			
			// Query actual data.
			String sqlStr = sql.buildSqlStatement(); 
			Log.info(sqlStr);
			pstmt = sql.getPreparedStatement(conn, sqlStr);
			ResultSet rset = pstmt.executeQuery();

			storedPstmt = pstmt;
			storedResultSet = rset;
			storedStream = stream;
		} catch (SQLException e) {
			if (pstmt != null)
				pstmt.close();
			String msg = e.getMessage();
			if (msg.contains("An illegal character has been found in the statement")) {
				throw new IllegalArgumentException(msg);
			} else {
				throw e;
			}
		}
	}

	private SqlBuilder processAggregate(Aggregator agg, SqlBuilder sql) throws SQLException, ClassNotFoundException {

		// Create row type if not exists.
		String aggRowType = Stream.getRowTypeName(agg.channels);
		createRowType(agg.channels);
		
		// Get temporary UUID
		String tempName = newUUIDString();
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Create temporary tables
		createTempStreamTable(tempTable, aggRowType);		
		createTempVirtualTable(tempVTable, tempTable);

		// Generate aggregate SQL
		String aggregateSql = null;
		if (agg.aggregator.equals(Aggregator.Type.AGGREGATE_BY)) {
			aggregateSql = generateAggregateBySql(tempTable, agg, aggRowType, sql);
		} else if (agg.aggregator.equals(Aggregator.Type.AGGREGATE_RANGE)) {
			aggregateSql = generateAggregateRangeSql(tempTable, sql, agg, aggRowType);
		} else {
			throw new IllegalStateException("Unknown aggregator type");
		}
		
		Log.info(aggregateSql);
		
		// Execute Aggregate SQL
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(aggregateSql);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			String msg = e.getMessage();
			if (msg.contains("aggregation is not allowed on columns of type")) {
				throw new IllegalArgumentException(e.getMessage());
			} else if (msg.contains("Invalid aggregation operator")) {
				throw new IllegalArgumentException(e.getMessage());
			} else {
				throw e;
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		} 
		
		// Save table names for Noisy aggreates.
		String aggTargetStreamTableName = sql.streamTableName;
		String aggTargetVirtualTableName = sql.virtualTableName;
		
		// Modify original sql to refer to the temporary virtual table
		sql.virtualTableName = tempVTable;
		sql.streamTableName = tempTable;	
		sql.stream.channels = agg.channels;

		// Add noise.
		if (agg.isNoisy()) {
			sql = processNoisyAggregate(sql, agg, aggTargetStreamTableName, aggTargetVirtualTableName);
		}

		// Time range condition no needed anymore since we create new temp table with the time range.
		sql.removeTimeRange();

		return sql;
	}

	private SqlBuilder processNoisyAggregate(SqlBuilder sql, Aggregator agg, String aggTargetStreamTableName, String aggTargetVirtualTableName) throws SQLException {

		// Get temporary UUID
		String tempName = newUUIDString();
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Create temporary tables
		String aggRowType = Stream.getRowTypeName(agg.channels);
		createTempStreamTable(tempTable, aggRowType);		
		createTempVirtualTable(tempVTable, tempTable);

		// Create an empty time series for aggregate noise result.
		String insertSql = "INSERT INTO " + tempTable + " VALUES ( " + sql.stream.id 
				+ ",'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";

		Log.info(insertSql);

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(insertSql);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) { 
				pstmt.close();
			}
		}

		// Get noise generatros
		List<DiffPrivNoiseGenerator> noiseGenerators = getNoiseGenerators(sql.stream, agg.epsilon); 

		// Add noise
		String selectSql = "SELECT * FROM " + sql.virtualTableName + " WHERE id = ?;";
		
		Log.info(selectSql);
		
		String noiseInsertSql = "INSERT INTO " + tempVTable + " VALUES (?,?,";
		for (int i = 1; i <= agg.channels.size(); i++) {
			noiseInsertSql += "?,";
		}
		noiseInsertSql = noiseInsertSql.substring(0, noiseInsertSql.length() - 1) + ")";
		
		Log.info(noiseInsertSql);
		
		pstmt = null;		
		PreparedStatement pstmt2 = null;
		try {
			pstmt = conn.prepareStatement(selectSql);
			pstmt2 = conn.prepareStatement(noiseInsertSql);
			pstmt.setInt(1, sql.stream.id);
			pstmt2.setInt(1, sql.stream.id);
			ResultSet rset = pstmt.executeQuery();
			Timestamp prevTs = null;
			List<Double> prevValues = null;
			while (rset.next()) {
				Timestamp timestamp = rset.getTimestamp(2);
				List<Double> values = new ArrayList<Double>();				
				int idx = 3;
				for (Channel channel : agg.channels) {
					if (channel.type.equals("float")) {
						values.add(rset.getDouble(idx++));
					} else {
						throw new IllegalStateException("Aggregate channel type is not float.");
					}
				}

				if (prevTs != null && prevValues != null) {
					long n = -1;
					if (agg.isAvgAggregator) {
						n = executeGetCountSql(aggTargetStreamTableName, sql.stream.id, prevTs, timestamp);
					}
					addNoiseAndExecuteUpdate(pstmt2, agg, prevTs, prevValues, noiseGenerators, n);
				}
				
				prevTs = timestamp;
				prevValues = values;
			}
			// Process last aggregate
			long n = -1;
			if (agg.isAvgAggregator) {
				n = executeGetCountSql(aggTargetStreamTableName, sql.stream.id, prevTs, sql.endTime);
			}
			addNoiseAndExecuteUpdate(pstmt2, agg, prevTs, prevValues, noiseGenerators, n);
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (pstmt2 != null) {
				pstmt2.close();
			}
		}
		
		sql.streamTableName = tempTable;
		sql.virtualTableName = tempVTable;
		
		return sql;
	}

	private long executeGetCountSql(String streamTableName, int streamId, Timestamp startTs, Timestamp endTs) throws SQLException {
		int numSamples;
		PreparedStatement pstmt = null;
		String countSql;
		if (startTs == null && endTs == null) {
			countSql = "SELECT GetNElems(tuples) FROM " + streamTableName + " WHERE id = ?";
		} else {
			countSql = "SELECT ClipGetCount(tuples,?,?) FROM " + streamTableName + " WHERE id = ?";	
		}
		Log.info(countSql);
		try {
			pstmt = conn.prepareStatement(countSql);
			if (startTs != null || endTs != null) {
				pstmt.setTimestamp(1, startTs);
				pstmt.setTimestamp(2, endTs);
				pstmt.setInt(3, streamId);
			} else {
				pstmt.setInt(1, streamId);
			}
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next()) {
				throw new IllegalStateException("ResultSet of ClipGetCount() is null.");
			}
			numSamples = rset.getInt(1);	
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
		Log.info(
				(startTs == null ? startTs : startTs.toString()) 
				+ " ~ " 
				+ (endTs == null ? endTs : endTs.toString()) 
				+ ": numSamples = " + numSamples);
		return numSamples;
	}

	private void addNoiseAndExecuteUpdate(PreparedStatement pstmt, Aggregator agg, Timestamp timestamp, List<Double> values, List<DiffPrivNoiseGenerator> noiseGenerators, long n) throws SQLException {
		addNoiseToValues(values, agg.channels, noiseGenerators, n);
		
		pstmt.setTimestamp(2, timestamp);
		for (int i = 3; i < agg.channels.size() + 3; i++) {
			pstmt.setDouble(i, values.get(i - 3));
		}
		pstmt.executeUpdate();
	}

	private List<DiffPrivNoiseGenerator> getNoiseGenerators(Stream stream, double epsilon) throws SQLException {
		// find out original channel's statistics
		Stream oriStream = getStream(stream.owner, stream.name);
		Map<String, Statistics> channelStatMap = new HashMap<String, Statistics>();
		for (Channel c : oriStream.channels) {
			channelStatMap.put(c.name, c.statistics);
		}
		
		// get noise generators
		Pattern patt = Pattern.compile("\\$[a-zA-Z0-9]+");
		List<DiffPrivNoiseGenerator> list = new ArrayList<DiffPrivNoiseGenerator>();
		for (Channel channel : stream.channels) {
			Matcher matcher = patt.matcher(channel.name);
			String oriChannelName; 
			if (matcher.find()) {
				oriChannelName = matcher.group().replace("$", "");
			} else {
				throw new IllegalStateException("Canot find matching channel name.");
			}
			Statistics stat = channelStatMap.get(oriChannelName);
			if (stat == null) {
				throw new IllegalStateException("Channel statistics is not ready yet.");
			} else {
				list.add(new DiffPrivNoiseGenerator(epsilon, stat.min, stat.max));
			}
		}
		return list;
	}

	private void addNoiseToValues(List<Double> values, List<Channel> channels, List<DiffPrivNoiseGenerator> noiseGenerators, long n) {
		for (int i = 0; i < values.size(); i++) {
			double value = values.get(i);
			double noise = getNoise(channels.get(i).name, noiseGenerators.get(i), n);
			Log.info("value:" + value + ", noise:" + noise);
			values.set(i, value + noise);
		}
	}
	
	private double getNoise(String name, DiffPrivNoiseGenerator noiseGenerator, long n) {
		name = name.toLowerCase();
		if (name.contains("avg")) {
			return noiseGenerator.getAvgNoise(n);
		} else if (name.contains("min") || name.contains("max")) {
			return noiseGenerator.getMinMaxNoise();
		} else if (name.contains("median")) {
			return noiseGenerator.getMedianNoise();
		} else if (name.contains("sum")) {
			return noiseGenerator.getSumNoise();
		} else if (name.contains("first") || name.contains("last") || name.contains("nth")) {
			return noiseGenerator.getNthNoise();
		} else {
			throw new IllegalStateException("Unknown aggregate expression: " + name);
		}
	}

	private String generateAggregateRangeSql(String tempTable, SqlBuilder sql, Aggregator agg, String aggRowType) throws SQLException {
		// Create an empty time series for aggregate result.
		String insertSql = "INSERT INTO " + tempTable + " VALUES ( " + sql.stream.id 
				+ ",'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";

		Log.info(insertSql);

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(insertSql);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) { 
				pstmt.close();
			}
		}

		String aggregateSql = "UPDATE " + tempTable + " SET tuples = "
						+ "PutElem(tuples,("
						+ "SELECT AggregateRange('" + agg.sqlExpression + "',tuples,0";

		if (sql.startTime != null && sql.endTime != null) {
			aggregateSql += ",'" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5),'"
					+ sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5)";
		}
		
		aggregateSql += ")::" + aggRowType 
				+ " FROM " + sql.streamTableName + " WHERE id = " + sql.stream.id + "))"
				+ " WHERE id = " + sql.stream.id + ";";
		
		return aggregateSql;
	}

	private String generateAggregateBySql(String tempTable, Aggregator agg, String aggRowType, SqlBuilder sql) {
		
		// Generate AggregateBy SQL
		String aggregateSql = "INSERT INTO " + tempTable 
				+ " SELECT " + sql.stream.id 
				+ ", AggregateBy("
				+ "'" + agg.sqlExpression + "',"
				+ "'" + agg.calendar + "',"
				+ "tuples,0";

		if (sql.startTime != null && sql.endTime != null) {
			aggregateSql += ",'" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5),"
					+ "'" + sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5)";
		}

		aggregateSql += ")::TimeSeries(" + aggRowType + ") "
				+ "FROM " + sql.streamTableName + " WHERE id = " + sql.stream.id + ";";
		
		return aggregateSql;
	}

	private SqlBuilder processFilterForAggregate(Aggregator agg, SqlBuilder sql) throws SQLException {

		// Limit the duration.
		checkStartEndTimeDuration(sql);

		//Get row type for this stream
		String rowtype = sql.stream.getRowTypeName();

		// Get temporary UUID
		String tempName = newUUIDString();
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Create temporary tables
		createTempStreamTable(tempTable, rowtype);		
		createTempVirtualTable(tempVTable, tempTable);

		// Create new empty timeseries
		String newTsSql = "INSERT INTO " + tempTable + " VALUES ( " + sql.stream.id 
				+ ",'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";

		Log.info(newTsSql);

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(newTsSql);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) { 
				pstmt.close();
			}
		}

		// Insert result of existing filter into the new timeseries
		pstmt = null;
		try {
			pstmt = sql.getPreparedStatementInsertPrefix(conn, tempVTable);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}

		// Change sql refernce tables
		sql.virtualTableName = tempVTable;
		sql.streamTableName = tempTable;

		// Remove conditions
		sql.removeConditions();

		return sql;
	}

	private void checkStartBeforeEndTime(DateTime start, DateTime end) {
		if (start != null && end != null) {
			if (start.isAfter(end)) {
				throw new IllegalArgumentException("start_time must be before end_time.");
			}
		}
	}

	private void checkStartEndTimeDuration(SqlBuilder sql) {
		if (sql.startTime == null || sql.endTime == null) {
			throw new IllegalArgumentException("Start time and end time are required for this query.");
		}

		// Limit the duration
		DateTime start = getDateTimeFromString(sql.startTime.toString());
		DateTime end = getDateTimeFromString(sql.endTime.toString());

		Duration duration = new Duration(start, end);

		if (duration.getStandardDays() > QUERY_DURATION_LIMIT) {
			throw new IllegalArgumentException("Duration limit between start_time and end_time is violated. Current limit is " + QUERY_DURATION_LIMIT + " days.");
		}
	}

	private DateTime getDateTimeFromString(String timestamp) {

		DateTime dt = null;

		try {
			DateTimeFormatter sqlFmtFraction = DateTimeFormat.forPattern(SQL_DATE_TIME_PATTERN_WITH_FRACTION);
			dt = sqlFmtFraction.parseDateTime(timestamp);
		} catch (IllegalArgumentException e) {
			try {
				DateTimeFormatter isoFmt = ISODateTimeFormat.dateTime();
				dt = isoFmt.parseDateTime(timestamp);
			} catch (IllegalArgumentException e1) {
				try {
					DateTimeFormatter sqlFmt = DateTimeFormat.forPattern(SQL_DATE_TIME_PATTERN);
					dt = sqlFmt.parseDateTime(timestamp);
				} catch (IllegalArgumentException e2) {
					throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_TIMESTAMP_FORMAT);
				}
			}
		}

		return dt;
	}

	private SqlBuilder processConditionOnOtherStreams(SqlBuilder sql, String streamOwner) throws SQLException {

		// Check if the sql contains expression: STREAM_NAME.CHANNEL_NAME
		Pattern cronExprPattern = Pattern.compile("\\s[a-zA-Z]+[a-zA-Z0-9_]*\\.[a-zA-Z]+[a-zA-Z0-9_]*\\s");
		Matcher matcher = cronExprPattern.matcher(sql.getWhereCondition());
		Map<Stream, Set<String>> otherStreamMap = new HashMap<Stream, Set<String>>();
		while (matcher.find()) {
			String expr = matcher.group();
			expr = expr.replace(" ", "");
			String[] splitExpr = expr.split("\\.");
			String otherStream = splitExpr[0];
			String otherChannel = splitExpr[1];

			if (otherStreamMap.containsKey(otherStream)) {
				otherStreamMap.get(otherStream).add(otherChannel);
			} else {
				Set<String> channelSet = new HashSet<String>();
				channelSet.add(otherChannel);
				otherStreamMap.put(getStream(streamOwner, otherStream), channelSet);				
			}
		}		

		// If no other stream expression, return original sql.
		if (otherStreamMap.isEmpty()) {
			return sql;
		}

		// Limit the duration
		checkStartEndTimeDuration(sql);

		// Get temporary table names
		String tempName = newUUIDString();
		String tempRowType = "rowtype_" + tempName;
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Get merged channels and conversion table.
		Map<String, String> conversion = new HashMap<String, String>();
		List<Channel> mergedChannel = new ArrayList<Channel>();
		int idx = sql.stream.channels.size() + 1;
		for (Stream otherStream: otherStreamMap.keySet()) {
			for (Channel channel: otherStream.channels) {
				mergedChannel.add(channel);
				conversion.put(otherStream.name + "." + channel.name, "channel" + idx);
				idx++;
			}
		}

		// Create row type for combined time series.
		String createRowtype = "CREATE ROW TYPE " + tempRowType + " ("
				+ "timestamp DATETIME YEAR TO FRACTION(5), ";
		idx = 1;
		for (Channel channel: sql.stream.channels){
			if (channel.type.equals("float")) {
				createRowtype += "channel" + idx + " FLOAT, ";
			} else if (channel.type.equals("int")) {
				createRowtype += "channel" + idx + " INT, ";
			} else if (channel.type.equals("text")) {
				createRowtype += "channel" + idx + " VARCHAR(255), ";
			} else {
				throw new UnsupportedOperationException(ExceptionMessages.MSG_UNSUPPORTED_TUPLE_FORMAT);
			}
			idx++;
		}
		for (Channel channel: mergedChannel) {
			if (channel.type.equals("float")) {
				createRowtype += "channel" + idx + " FLOAT, ";
			} else if (channel.type.equals("int")) {
				createRowtype += "channel" + idx + " INT, ";
			} else if (channel.type.equals("text")) {
				createRowtype += "channel" + idx + " VARCHAR(255), ";
			} else {
				throw new UnsupportedOperationException(ExceptionMessages.MSG_UNSUPPORTED_TUPLE_FORMAT);
			}
			idx++;
		}
		createRowtype = createRowtype.substring(0, createRowtype.length() - 2) + ")";

		Log.info(createRowtype);

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(createRowtype);
			pstmt.execute();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		storedTempRowTypeName = tempRowType;

		// Create temporary stream table.
		createTempStreamTable(tempTable, tempRowType);

		// Create temporary virtual table.
		createTempVirtualTable(tempVTable, tempTable);

		// Generate Union() sql
		String executeUnion = "INSERT INTO " + tempTable + " SELECT " + sql.stream.id + ", Union('" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5), '" + sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5), t1.tuples, ";

		for (idx = 2; idx <= otherStreamMap.keySet().size() + 1; idx++) {
			executeUnion += "t" + idx + ".tuples, ";
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 2) + ")::TimeSeries(" + tempRowType + ") FROM ";
		executeUnion += sql.stream.getStreamTableName() + " t1, "; 
		idx = 2;
		for (Stream otherStream: otherStreamMap.keySet()) {
			executeUnion += otherStream.getStreamTableName() + " t" + idx + ", ";
			idx++;
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 2) + " WHERE ";
		executeUnion += "t1.id = " + sql.stream.id + " AND ";
		idx = 2;
		for (Stream otherStream: otherStreamMap.keySet()) {
			executeUnion += "t" + idx + ".id = " + otherStream.id + " AND ";
			idx++;
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 5) + ";";

		Log.info(executeUnion);

		// Execute Union()
		pstmt = null;
		try {
			pstmt = conn.prepareStatement(executeUnion);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}

		// Change SQL to refer temporary table.
		sql.virtualTableName = tempVTable;
		sql.streamTableName = tempTable;

		// Remove time range because it is already processed.
		sql.removeTimeRange();

		// Replace channel name on other streams.
		for (Entry<String, String> entry: conversion.entrySet()) {
			Log.info(entry.getKey() + ", " + entry.getValue());
			if (sql.condFilter != null) {				
				sql.condFilter = sql.condFilter.replace(entry.getKey(), entry.getValue());
			}
			if (sql.condRules != null) {
				sql.condRules = sql.condRules.replace(entry.getKey(), entry.getValue());
			}
		}

		return sql;
	}

	private void createTempVirtualTable(String tempVTable, String tempTable) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "EXECUTE PROCEDURE TSCreateVirtualTab(?,?)";
			Log.info(sql);
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, tempVTable);
			pstmt.setString(2, tempTable);
			pstmt.execute();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}		
		storedTempTables.add(tempVTable);
	}

	private void createTempStreamTable(String tempTable, String tempRowType) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "CREATE TEMP TABLE " + tempTable + " ( id int, tuples TimeSeries(" + tempRowType + "))";
			Log.info(sql);
			pstmt = conn.prepareStatement(sql);
			pstmt.execute();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private void dropTemps() throws SQLException {
		dropStoredTempTables();
		dropStoredTempView();
		dropStoredTempRowTypeName();
	}

	private void cleanUpStoredInfo() throws SQLException {
		if (storedPstmt != null) { 
			storedPstmt.close();
		}
		dropTemps();
		storedResultSet = null;
		storedPstmt = null;
		storedStream = null;
	}

	@Override
	public Object[] getNextTuple() throws SQLException {
		if (storedResultSet == null) {
			cleanUpStoredInfo();
			return null;
		}

		if (storedResultSet.isClosed() || storedResultSet.isAfterLast()) {
			cleanUpStoredInfo();
			return null;
		}

		if (storedResultSet.next()) {
			Object[] tuple = new Object[storedStream.channels.size() + 1];
			tuple[0] = storedResultSet.getTimestamp(2).getTime(); // epoch in ms.
			int startColIdx = 3;
			for (int i = 0; i < storedStream.channels.size(); i++) {
				tuple[i + 1] = storedResultSet.getObject(startColIdx + i);
			}
			return tuple;
		} else {
			cleanUpStoredInfo();
			return null;
		}
	}
	
	private String getRuleCondition(String streamOwner, String requestingUser, Stream stream) throws SQLException {

		PreparedStatement pstmt = null;
		String ruleCond = null;
		try {
			String sql = "SELECT priority, condition, action "
					+ "FROM rules "
					+ "WHERE owner = ? "
					+ "AND is_aggregator = ?"
					+ "AND ( ? IN target_streams OR target_streams IS NULL) "
					+ "AND ( ? IN target_users OR target_users IS NULL ) "
					+ "AND template_name IS NULL "
					+ "ORDER BY priority DESC;";

			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, streamOwner);
			pstmt.setBoolean(2, false);
			pstmt.setString(3, stream.name);
			pstmt.setString(4, requestingUser);

			ResultSet rset = pstmt.executeQuery();

			int prevPriority = -1;
			List<String> allowConds = new ArrayList<String>();
			List<String> denyConds = new ArrayList<String>();
			while (rset.next()) {
				String action = rset.getString(3);

				int priority = rset.getInt(1);
				String condition = rset.getString(2);

				if (prevPriority == -1) {
					prevPriority = priority; 
				}

				// Collect conditions in same priority
				if (prevPriority == priority) {
					addCurrentRule(allowConds, denyConds, action, condition);
				} else {
					// If priority changes, merge and add to ruleCond.

					if (ruleCond == null && allowConds.isEmpty()) {
						// Ignore only deny rules with the least priority
						denyConds.clear();
						prevPriority = priority;
						addCurrentRule(allowConds, denyConds, action, condition);
						continue;
					}

					ruleCond = addConditionsToRuleCond(ruleCond, allowConds, denyConds);

					// Process current priority.
					allowConds.clear();
					denyConds.clear();
					addCurrentRule(allowConds, denyConds, action, condition);
				}

				prevPriority = priority;
			}

			// Process final rules
			ruleCond = addConditionsToRuleCond(ruleCond, allowConds, denyConds);		
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}

		return ruleCond;
	}

	private String addConditionsToRuleCond(String ruleCond, List<String> allowConds, List<String> denyConds) {
		if (denyConds.contains("deny all")) {
			return null;
		}
		boolean isAllowAll = false;
		if (allowConds.contains("allow all")) {
			isAllowAll = true;
			allowConds.remove("allow all");
		}
		if (!allowConds.isEmpty()) {			
			if (ruleCond == null) { 
				ruleCond = "( " + StringUtils.join(allowConds, " OR ") + " )";
			} else {
				if (isAllowAll) {
					ruleCond = "( " + StringUtils.join(allowConds, " OR ") + " )";
				} else {
					if (ruleCond.equals("allow all")) {
						ruleCond = "( " + StringUtils.join(allowConds, " OR ") + " )";
					} else {
						ruleCond = "( " + ruleCond + " ) OR ( " + StringUtils.join(allowConds, " OR ") + " )";	
					}
				}
			}
		}
		if (!denyConds.isEmpty()) {
			if (ruleCond == null) {
				assert false;
			} else {
				if (ruleCond.equals("allow all")) {
					ruleCond = "NOT ( " + StringUtils.join(denyConds, " OR ") + " )";
				} else {
					ruleCond = "( " + ruleCond + " ) AND NOT ( " + StringUtils.join(denyConds, " OR ") + " )";
				}
			}
		}
		if (ruleCond == null && isAllowAll) {
			ruleCond = "allow all";
		}
		return ruleCond;
	}

	private void addCurrentRule(List<String> allowConds, List<String> denyConds, String action, String condition) {
		if (action.equalsIgnoreCase("allow")) {
			if (condition == null) {
				allowConds.add("allow all");
			} else {
				allowConds.add("( " + condition + " )");
			}
		} else if (action.equalsIgnoreCase("deny")) {
			if (condition == null) {
				denyConds.add("deny all");
			} else {
				denyConds.add("( " + condition + " )");
			}
		}
	}

	@Override
	public Stream getStream(String owner, String streamName) throws SQLException {
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		Stream stream = null;
		try {
			String sql = "SELECT tags, channels, id FROM streams WHERE owner = ? AND name = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, streamName);
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next()) {
				throw new IllegalArgumentException("No such stream (" + streamName + ") of owner (" + owner + ")");
			}
			String tags = rset.getString(1);
			Array channels = rset.getArray(2);			
			int id = rset.getInt(3);
			stream = new Stream(id, streamName, owner, tags, channels);
			
			pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + stream.getStreamTableName() + " WHERE id = ?");
			pstmt2.setInt(1, id);
			ResultSet rset2 = pstmt2.executeQuery();
			if (!rset2.next()) {
				throw new IllegalStateException("ResultSet is null.");
			}
			int numSamples = rset2.getInt(1);
			stream.setNumSamples(numSamples);
			
			// get channel statistics
			for (Channel channel : stream.channels) {
				getChannelStatistics(conn, stream, channel);
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (pstmt2 != null)
				pstmt2.close();
		}		

		return stream;
	}

	@Override
	public List<Stream> getStreamList(String owner) throws SQLException {
		return getStreamList(conn, owner);
	}

	private static List<Stream> getStreamList(Connection conn, String owner) throws SQLException {
		List<Stream> streams;
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		try {
			if (owner != null) {
				pstmt = conn.prepareStatement("SELECT id, name, tags, channels FROM streams WHERE owner = ?");
				pstmt.setString(1, owner);
			} else {
				pstmt = conn.prepareStatement("SELECT id, name, tags, channels FROM streams");
			}
			ResultSet rSet = pstmt.executeQuery();
			streams = new LinkedList<Stream>();
			while (rSet.next()) {
				int id = rSet.getInt(1);
				Array channels = rSet.getArray(4);
				Stream stream = new Stream(id, rSet.getString(2), owner, rSet.getString(3), channels);
				
				pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + stream.getStreamTableName() + " WHERE id = ?");
				pstmt2.setInt(1, id);
				ResultSet rset2 = pstmt2.executeQuery();
				if (!rset2.next()) {
					throw new IllegalStateException("ResultSet is null.");
				}
				int numSamples = rset2.getInt(1);
				stream.setNumSamples(numSamples);
				
				// get channel statistics
				for (Channel channel : stream.channels) {
					getChannelStatistics(conn, stream, channel);
				}
				
				streams.add(stream);
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (pstmt2 != null)
				pstmt2.close();
		}

		return streams;
	}

	private static void getChannelStatistics(Connection conn, Stream stream, Channel channel) throws SQLException {
		String selectSql = "SELECT min_value, max_value FROM channel_statistics WHERE id = ? AND channel_name = ?;";
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(selectSql);
			pstmt.setInt(1, stream.id);
			pstmt.setString(2, channel.name);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				channel.statistics = new Statistics(rset.getDouble(1), rset.getDouble(2));
			} 
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	@Override
	public void deleteStream(String owner, String streamName, String startTime, String endTime) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			// Get stream information.
			String sql = "SELECT id, channels FROM streams WHERE owner = ? AND name = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, streamName);
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next()) {
				throw new IllegalArgumentException("No such stream (" + streamName + ").");
			}
			
			int id = rset.getInt(1);
			String prefix = Stream.getChannelFormatPrefix(Stream.getListChannelFromSqlArray(rset.getArray(2)));

			if (pstmt != null) {
				pstmt.close();
			}

			// Delete a stream.
			if (startTime == null && endTime == null) {
				sql = "DELETE FROM streams WHERE id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, id);
				pstmt.executeUpdate();
				pstmt.close();

				sql = "DELETE FROM " + prefix + "streams WHERE id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, id);
				pstmt.executeUpdate();

				// Delete some portions of the stream.
			} else if (startTime != null && endTime != null) {
				Timestamp startTs = null, endTs = null;
				try {
					if (startTime != null) 
						startTs = Timestamp.valueOf(startTime);
					if (endTime != null)
						endTs = Timestamp.valueOf(endTime);
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_TIMESTAMP_FORMAT);
				}

				sql = "UPDATE " + prefix + "streams SET tuples = "
						+ "DelRange(tuples, ?, ?) WHERE id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setTimestamp(1, startTs);
				pstmt.setTimestamp(2, endTs);
				pstmt.setInt(3, id);
				pstmt.executeUpdate();
			} else {
				throw new IllegalArgumentException("Both the query parameters start_time and end_time or none of them should be provided.");
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteAllStreams(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		try {
			pstmt = conn.prepareStatement("SELECT id, channels FROM streams WHERE owner = ?");
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				int id = rset.getInt(1);
				String prefix = Stream.getChannelFormatPrefix(Stream.getListChannelFromSqlArray(rset.getArray(2)));
				String sql = "DELETE FROM " + prefix + "streams WHERE id = ?";
				pstmt2 = conn.prepareStatement(sql);
				pstmt2.setInt(1, id);
				pstmt2.executeUpdate();
			}
			pstmt.close();
			pstmt = conn.prepareStatement("DELETE FROM streams WHERE owner = ?");
			pstmt.setString(1, owner);
			pstmt.execute();
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (pstmt2 != null)
				pstmt.close();
		}
	}

	private String findDelimiter(String line) {
		String[] cols = line.split(",");
		if (cols.length > 1) {
			return ",";
		}

		cols = line.split("\t");
		if (cols.length > 1) {
			return "\t";
		}

		throw new IllegalArgumentException("Unable to determine column delimiter. Supported delimiters are tab or comma.");
	}

	private DateTimeFormatter findDateTimeFormat(String[] lines, String delimiter) {
		DateTimeFormatter isoFmt = ISODateTimeFormat.dateTime();
		DateTimeFormatter sqlFmt = DateTimeFormat.forPattern(SQL_DATE_TIME_PATTERN);
		DateTimeFormatter sqlFmtFraction = DateTimeFormat.forPattern(SQL_DATE_TIME_PATTERN_WITH_FRACTION);
		DateTimeFormatter returnFmt = null;

		String timestamp = lines[0].split(delimiter, 2)[0];
		@SuppressWarnings("unused")
		DateTime dt = null;
		try {
			dt = sqlFmtFraction.parseDateTime(timestamp);
			returnFmt = sqlFmtFraction;
		} catch (IllegalArgumentException e) {
			try {
				dt = isoFmt.parseDateTime(timestamp);
				returnFmt = isoFmt;
			} catch (IllegalArgumentException e1) {
				try {
					dt = sqlFmt.parseDateTime(timestamp);
					returnFmt = sqlFmt;
				} catch (IllegalArgumentException e2) {
					// If first line fails, try for 2nd line because CSV file might have header line.
					lines[0] = null;
					timestamp = lines[1].split(delimiter, 2)[0];
					try {
						dt = sqlFmtFraction.parseDateTime(timestamp);
						returnFmt = sqlFmtFraction;
					} catch (IllegalArgumentException e3) {
						try {
							dt = isoFmt.parseDateTime(timestamp);
							returnFmt = isoFmt;
						} catch (IllegalArgumentException e4) {
							try {
								dt = sqlFmt.parseDateTime(timestamp);
								returnFmt = sqlFmt;
							} catch (IllegalArgumentException e5) {
							}
						}
					}
				}
			}
		}

		if (returnFmt == null) {
			throw new IllegalArgumentException("Unsupported date time format.  Supported formats are ISO 8601 or SQL standard format (yyyy-MM-dd HH:mm:ss.SSSSS).");
		}

		return returnFmt;
	}

	private File createBulkloadFileAndUpdatePendingStatistics(Stream stream, String data) throws IOException, NoSuchAlgorithmException {
		FileWriter fw = null;
		BufferedWriter bw = null;
		File file = null;

		try {
			String fileName = BULK_LOAD_DATA_FILE_NAME_PREFIX + newUUIDString();
			file = new File(fileName);
			if (file.exists()) {
				file.delete();				
			}
			file.createNewFile();
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);

			String lineSeparator = System.getProperty("line.separator");
			String[] lines = data.split(lineSeparator);

			// Determine delimiter
			String delimiter = findDelimiter(lines[0]);

			// Determine date time format
			DateTimeFormatter fmt = findDateTimeFormat(lines, delimiter);
			DateTimeFormatter sqlFmt = DateTimeFormat.forPattern(SQL_DATE_TIME_PATTERN);

			for (String line: lines) {
				if (line == null) {
					continue;
				}

				String[] cols = line.split(delimiter, 2);
				String timestamp = cols[0];
				String values = cols[1];

				DateTime dt = null;
				try {
					dt = fmt.parseDateTime(timestamp);
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException("Unable to parse timestamp: " + timestamp);
				}

				updatePendingStatistics(stream, values.split(delimiter));
				
				bw.write(dt.toString(sqlFmt) + delimiter + values + lineSeparator);
			}
		} finally {
			if (bw != null)
				bw.close();
			if (fw != null)
				fw.close();
		}

		return file;
	}

	private void updatePendingStatistics(Stream stream, String[] values) {
		int i = 0;
		for (Channel c : stream.channels) {
			if (isNumeric(c)) {
				if (c.statistics == null) {
					throw new IllegalStateException("Channel statistics is null.");
				} else {
					double min = c.statistics.min;
					double max = c.statistics.max;
					double value;
					if (c.type.equals("float")) {
						value = Double.valueOf(values[i]);
					} else if (c.type.equals("int")) {
						value = Integer.valueOf(values[i]);
					} else {
						throw new IllegalStateException("Invalid channel type.");
					}
					if (min > value) {
						c.statistics.min = value;
					}
					if (max < value) {
						c.statistics.max = value;
					}
				}
			}
			i++;
		}
	}

	@Override
	public void bulkLoad(String ownerName, String streamName, String data) throws SQLException, IOException, NoSuchAlgorithmException {
		Stream stream = getStream(ownerName, streamName);
		
		// new channel statistics will be stored in stream.channels
		File file = createBulkloadFileAndUpdatePendingStatistics(stream, data);
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = conn.prepareStatement("BEGIN WORK");
			pstmt.execute();
			pstmt.close();

			pstmt = conn.prepareStatement("UPDATE " + stream.getStreamTableName() + " SET tuples = BulkLoad(tuples, ?, ?) WHERE id = ?");
			pstmt.setString(1, file.getAbsolutePath());
			pstmt.setInt(2, TSOPEN_REDUCED_LOG);
			pstmt.setInt(3, stream.id);
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = conn.prepareStatement("COMMIT WORK");
			pstmt.execute();

			file.delete();
			
		} catch (SQLException e) {
			if (e.getMessage().contains("Too many data values")) {
				throw new IllegalArgumentException(e.getMessage());
			} else {
				throw e;
			}
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
		
		// update channel statistics
		Stream oriStream = getStream(ownerName, streamName);
		
		for (int i = 0; i < oriStream.channels.size(); i++) {
			double oriMin = oriStream.channels.get(i).statistics.min;
			double oriMax = oriStream.channels.get(i).statistics.max;
			double newMin = stream.channels.get(i).statistics.min;
			double newMax = stream.channels.get(i).statistics.max;
			if (oriMin != newMin) {
				updateChannelStatisticsMin(streamName, stream.channels.get(i).name, newMin);
			}
			if (oriMax != newMax) {
				updateChannelStatisticsMax(streamName, stream.channels.get(i).name, newMax);
			}
		}
	}

	@Override
	public void addOrUpdateMacro(String owner, Macro macro) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "SELECT macro_name FROM macros WHERE macro_name = ?;";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, macro.name);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				pstmt.close();
				sql = "UPDATE macros SET macro_value = ? WHERE macro_name = ?;";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, macro.value);
				pstmt.setString(2, macro.name);
				pstmt.executeUpdate();
			} else {
				pstmt.close();
				sql = "INSERT INTO macros (owner, macro_name, macro_value) VALUES (?,?,?);";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, owner);
				pstmt.setString(2, macro.name);
				pstmt.setString(3, macro.value);
				pstmt.executeUpdate();
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public List<Macro> getMacros(String owner) throws SQLException {
		List<Macro> macros = new ArrayList<Macro>();

		PreparedStatement pstmt = null;
		try {
			String sql = "SELECT macro_name, macro_value, id FROM macros WHERE owner = ?;";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				macros.add(new Macro(rset.getString(1), rset.getString(2), rset.getInt(3)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}

		return macros;
	}

	@Override
	public void deleteAllMacros(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM macros WHERE owner = ?");
			pstmt.setString(1, owner);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteMacro(String owner, int id, String name) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "DELETE FROM macros WHERE owner = ?";

			if (id > 0) {
				sql += " AND id = ?";
			}
			if (name != null) {
				sql += " AND macro_name = ?";
			}

			pstmt = conn.prepareStatement(sql);

			pstmt.setString(1, owner);

			int i = 2;
			if (id > 0) {
				pstmt.setInt(i, id);
				i++;
			}
			if (name != null) {
				pstmt.setString(i, name);
				i++;
			}

			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteTemplate(String ownerName, int id, String templateName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "DELETE FROM rules WHERE owner = ?";

			if (id > 0) {
				sql += " AND id = ?";
			}
			if (templateName != null) {
				sql += " AND template_name = ?";
			}

			pstmt = conn.prepareStatement(sql);

			pstmt.setString(1, ownerName);

			int i = 2;
			if (id > 0) {
				pstmt.setInt(i, id);
				i++;
			}
			if (templateName != null) {
				pstmt.setString(i, templateName);
				i++;
			}

			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteAllTemplates(String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM rules WHERE owner = ? AND template_name IS NOT NULL");
			pstmt.setString(1, ownerName);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public List<Rule> getTemplates(String ownerName) throws SQLException {
		PreparedStatement pstmt = null;
		List<Rule> templates = new ArrayList<Rule>();
		try {
			String sql = "SELECT id, target_users, target_streams, condition, action, priority, template_name FROM rules WHERE owner = ? AND template_name IS NOT NULL";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, ownerName);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				int id = rset.getInt(1);
				Array sqlArr = rset.getArray(2);
				Object[] targetUsers = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				sqlArr = rset.getArray(3);
				Object[] targetStreams = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				templates.add(new Rule(id, targetUsers, targetStreams, rset.getString(4), rset.getString(5), rset.getInt(6), rset.getString(7)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return templates;
	}

	@Override
	public void createRuleFromTemplate(String ownerName, TemplateParameterDefinition params) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "SELECT target_users, target_streams, condition, action, priority FROM rules WHERE owner = ? AND template_name = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, ownerName);
			pstmt.setString(2, params.template_name);
			ResultSet rset = pstmt.executeQuery();
			if (rset.next()) {
				Array templateTargetUsers = rset.getArray(1);
				Array templateTargetStreams = rset.getArray(2);
				String condition = applyTemplateParams(params.parameters, rset.getString(3));
				String action = rset.getString(4);
				pstmt.close();

				int priority = params.priority;
				sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, priority) VALUES (?,?,?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setString(1, ownerName);
				if (params.target_users == null) {
					pstmt.setArray(2, templateTargetUsers);
				} else {
					String targetUsers = SqlBuilder.getSqlSetString(params.target_users);
					pstmt.setString(2, targetUsers);
				}
				if (params.target_streams == null) {
					pstmt.setArray(3,  templateTargetStreams);
				} else {
					String targetStreams = SqlBuilder.getSqlSetString(params.target_streams);
					pstmt.setString(3, targetStreams);
				}
				pstmt.setString(4, condition);
				pstmt.setString(5, action);
				pstmt.setInt(6, priority);
				pstmt.executeUpdate();
			} else {
				throw new IllegalArgumentException("Template (" + params.template_name + ") doesn't exists.");						
			}
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private String applyTemplateParams(List<TemplateParameter> params, String expr) {
		for (TemplateParameter param: params) {
			expr = expr.replace("$(" + param.name + ")", param.value);
		}
		return expr;
	}

	@Override
	public Stream getStoredStreamInfo() {
		return storedStream;
	}
	
	@Override
	public ResultSet getStoredResultSet() {
		return storedResultSet;
	}
}
