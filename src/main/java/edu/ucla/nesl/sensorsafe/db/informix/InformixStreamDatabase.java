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

import com.informix.timeseries.IfmxTimeSeries;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.model.TemplateParameter;
import edu.ucla.nesl.sensorsafe.model.TemplateParameterDefinition;

public class InformixStreamDatabase extends InformixDatabaseDriver implements StreamDatabaseDriver {

	public InformixStreamDatabase() throws SQLException, IOException,
	NamingException, ClassNotFoundException {
		super();
	}

	private static final String ORIGIN_TIMESTAMP = "2000-01-01 00:00:00.00000";
	private static final String VALID_TIMESTAMP_FORMAT = "\"YYYY-MM-DD HH:MM:SS.[SSSSS]\"";
	private static final String[] VALID_RULE_ACTIONS = { "allow", "deny" }; 

	private static final String MSG_INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format. Expected format is " + VALID_TIMESTAMP_FORMAT;
	private static final String MSG_INVALID_CRON_EXPRESSION = "Invalid cron expression. Expects [ sec(0-59) min(0-59) hour(0-23) day of month(1-31) month(1-12) day of week(0-6,Sun-Sat) ]";
	private static final String MSG_UNSUPPORTED_TUPLE_FORMAT = "Unsupported tuple format.";
	private static final String MSG_INVALID_CALENDAR_TYPE = "Invalid calendar type. Supported types are: 1min, 15min, 30min, 1hour, 1day, 1week, 1month, 1year";
	private static final String MSG_INVALID_AGGREGATOR_EXPRESSION = "Invalid aggregator expression.";
	
	private static final String DATETIME_SECOND = "timestamp::DATETIME SECOND TO SECOND::CHAR(2)::INT";
	private static final String DATETIME_MINUTE = "timestamp::DATETIME MINUTE TO MINUTE::CHAR(2)::INT";
	private static final String DATETIME_HOUR = "timestamp::DATETIME HOUR TO HOUR::CHAR(2)::INT";
	private static final String DATETIME_DAY = "DAY(timestamp)";
	private static final String DATETIME_MONTH = "MONTH(timestamp)";
	private static final String DATETIME_WEEKDAY = "WEEKDAY(timestamp)";
	private static final String DATETIME_YEAR = "YEAR(timestamp)";
	private static final String[] SQL_DATETIME_CONVERSIONS = { DATETIME_SECOND, DATETIME_MINUTE, DATETIME_HOUR, DATETIME_DAY, DATETIME_MONTH, DATETIME_WEEKDAY, DATETIME_YEAR };

	private static final String BULK_LOAD_DATA_FILE_NAME_PREFIX = "/tmp/bulkload_data_";

	private static final String SQL_DATE_TIME_PATTERN_WITH_FRACTION = "yyyy-MM-dd HH:mm:ss.SSSSS";
	private static final String SQL_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

	private static final String CRON_TIME_REGULAR_EXPR = "\\[[\\d\\s-,\\*]+\\]";

	private static final long QUERY_DURATION_LIMIT = 7;

	// Informix TimeSeries flags
	private static final int TSOPEN_REDUCED_LOG = 256;

	private PreparedStatement storedPstmt;
	private ResultSet storedResultSet;
	private Stream storedStream;
	private List<String> storedTempTables = new ArrayList<String>();
	private String storedTempViewName;
	private String storedTempRowTypeName;

	static {
		try {
			initializeDatabase();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	private void dropStoredTempRowTypeName() throws SQLException {
		if (storedTempRowTypeName != null) {
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP ROW TYPE IF EXISTS " + storedTempRowTypeName + " RESTRICT;");				
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
					stmt.execute("DROP TABLE IF EXISTS " + table);				
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
				stmt.execute("DROP VIEW IF EXISTS " + storedTempViewName);				
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
					String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(1)));
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
				createStreamsTable();
			}

			// Check rules table
			pstmt.setString(1, "rules");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createRulesTable();
			}

			// Check macros table
			pstmt.setString(1, "macros");
			rset = pstmt.executeQuery();
			if (!rset.next()) {
				createMacrosTable();
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
				String typeName = "timeseries(" + getRowTypeName(getListChannelFromSqlArray(rset.getArray(1))) + ")";
				if (!typeMap.containsKey(typeName)) {
					typeMap.put(typeName, Class.forName("com.informix.timeseries.IfmxTimeSeries"));
				}
			}			
			conn.setTypeMap(typeMap);
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (stmt != null) 
				stmt.close();
			if (conn != null)
				conn.close();
		}

	}

	private static void createStreamsTable() throws SQLException {

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
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
			if (conn != null)
				conn.close();
		}
	}

	private static void createRulesTable() throws SQLException {

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE rules ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "priority BIGINT, "
					+ "target_users SET(VARCHAR(100) NOT NULL), "
					+ "target_streams SET(VARCHAR(100) NOT NULL), "
					+ "condition VARCHAR(255) NOT NULL, "
					+ "action VARCHAR(255) NOT NULL, "
					+ "template_name VARCHAR(255)) LOCK MODE ROW;");
		} 
		finally {
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		}
	}

	private static void createMacrosTable() throws SQLException {

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
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
			if (conn != null)
				conn.close();
		}
	}

	private static List<Channel> getListChannelFromSqlArray(Array sqlArray) throws SQLException {
		List<Channel> channels = new LinkedList<Channel>();
		Object[] objArray = (Object[])sqlArray.getArray();
		for (Object obj: objArray) {
			Struct struct = (Struct)obj;
			Object[] attr = struct.getAttributes();
			channels.add(new Channel((String)attr[0], (String)attr[1]));
		}
		return channels;
	}

	private static String getChannelFormatPrefix(List<Channel> channels) {
		String prefix = "";
		for (Channel channel: channels) {
			prefix += channel.type + "_";
		}
		return prefix;
	}

	private void validateRule(Rule rule) {
		if (rule.action == null) {
			throw new IllegalArgumentException("action is null.");
		}
		boolean isValid = false;
		for (String validAction: VALID_RULE_ACTIONS) {
			if (rule.action.equalsIgnoreCase(validAction)) {
				isValid = true;
			}
		}
		if (!isValid) {
			throw new IllegalArgumentException("Invalid rule action. Valid actions are: " + StringUtils.join(VALID_RULE_ACTIONS, ", "));
		}
		isValid = false;
		if (rule.priority < 1) {
			throw new IllegalArgumentException("priority must be greater than or equal to 1.");
		}
	}

	private String getSqlSetStringFromList(List<String> list) {
		String sqlSet = null;
		if (list != null) {
			sqlSet = "SET{";
			for (String item: list){
				sqlSet += "'" + item + "', ";
			}
			sqlSet = sqlSet.substring(0, sqlSet.length() - 2);
			sqlSet += "}";
		}
		return sqlSet;
	}

	@Override
	public void addUpdateRuleTemplate(String owner, Rule rule) throws SQLException {
		validateRule(rule);

		String targetUsers = getSqlSetStringFromList(rule.target_users);
		String targetStreams = getSqlSetStringFromList(rule.target_streams);

		PreparedStatement pstmt = null;
		if (rule.template_name != null) {
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

		try {
			String sql;
			if (rule.id == 0) {
				sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, priority, template_name) VALUES (?,?,?,?,?,?,?)";
			} else {
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
							+ "template_name = ? WHERE id = ?";
				} else {
					sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, id, priority, template_name) VALUES (?,?,?,?,?,?,?,?)";
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
			if (rule.id != 0) {
				pstmt.setInt(6, rule.id);
				pstmt.setInt(7, rule.priority);
				pstmt.setString(8, rule.template_name);
			} else {
				pstmt.setInt(6, rule.priority);
				pstmt.setString(7, rule.template_name);
			}
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	@Override
	public List<Rule> getRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		List<Rule> rules = new ArrayList<Rule>();
		try {
			String sql = "SELECT id, target_users, target_streams, condition, action, priority FROM rules WHERE owner = ? AND template_name IS NULL";
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
	public void createStream(String owner, Stream stream) throws SQLException, ClassNotFoundException {

		// Check if tupleFormat is valid
		if (!isChannelsValid(stream.channels))
			throw new IllegalArgumentException("Invalid channel definition."); 

		// Check if stream name exists.
		if ( isStreamNameExist(owner, stream.name) )
			throw new IllegalArgumentException("Stream name (" + stream.name + ") already exists.");

		// Get new stream id number.
		//int id = getNewStreamId();

		// Create row type if not exists.
		executeSqlCreateRowType(stream.channels);

		// Add the type map information
		Map<String, Class<?>> typeMap = conn.getTypeMap();
		String typeName = "timeseries(" + getChannelFormatPrefix(stream.channels)+ "rowtype)";
		if (!typeMap.containsKey(typeName)) {
			typeMap.put(typeName, Class.forName("com.informix.timeseries.IfmxTimeSeries"));
			conn.setTypeMap(typeMap);
		}

		// Create stream table if not exists.
		executeSqlCreateStreamTable(stream.channels);

		// Insert into streams table
		int newStreamId = executeSqlInsertIntoStreamTable(owner, stream);

		// Insert into timeseries streams table
		executeSqlInsertIntoTimeseriesTable(newStreamId, stream.channels);

		// Create a virtual table.
		executeSqlCreateVirtualTable(stream);
	}

	private boolean isChannelsValid(List<Channel> channels) {
		for (Channel channel: channels) {
			if (!channel.type.equals("float")
					&& !channel.type.equals("int")
					&& !channel.type.equals("text")) {
				return false;
			}
		}
		return true;
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

	private void executeSqlCreateRowType(List<Channel> channels) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			String rowTypeName = getRowTypeName(channels);
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

			stmt.execute(sql);
		} finally {
			if (stmt != null) 
				stmt.close();
		}
	}	

	private static String getRowTypeName(List<Channel> channels) {
		return getChannelFormatPrefix(channels) + "rowtype";
	}

	private void executeSqlCreateStreamTable(List<Channel> channels) throws SQLException {
		String rowtype = getRowTypeName(channels);
		String table = getStreamTableName(channels);
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "CREATE TABLE IF NOT EXISTS " + table 
					+ " (id BIGINT NOT NULL PRIMARY KEY, tuples TIMESERIES(" + rowtype + ")) LOCK MODE ROW";
			stmt.execute(sql);
		} finally {
			if (stmt != null) 
				stmt.close();
		}
	}

	private String getStreamTableName(List<Channel> channels) {
		return getChannelFormatPrefix(channels) + "streams";
	}

	private int executeSqlInsertIntoStreamTable(String owner, Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("INSERT INTO streams (owner, name, tags, channels) VALUES (?,?,?,?)");
			pstmt.setString(1, owner);
			pstmt.setString(2, stream.name);
			pstmt.setString(3, stream.tags);
			pstmt.setString(4, getChannelsSqlDataString(stream.channels));
			pstmt.executeUpdate();
			pstmt.close();

			pstmt = conn.prepareStatement("SELECT id FROM streams WHERE owner=? AND name=?");
			pstmt.setString(1, owner);
			pstmt.setString(2, stream.name);
			ResultSet rset = pstmt.executeQuery();
			rset.next();

			return rset.getInt(1);
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private String getChannelsSqlDataString(List<Channel> channels) {
		String ret = "LIST{";
		for (Channel ch: channels) {
			String str = "ROW('" + ch.name + "', '" + ch.type + "')";
			ret += str + ", ";
		}
		ret = ret.substring(0, ret.length() -2);
		ret += "}";
		return ret;
	}

	private void executeSqlInsertIntoTimeseriesTable(int newStreamId, List<Channel> channels) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String table = getStreamTableName(channels);
			String sql = "INSERT INTO " + table + " VALUES (?, "
					+ "'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, newStreamId);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	private void executeSqlCreateVirtualTable(Stream stream) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			String prefix = getChannelFormatPrefix(stream.channels);
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

		PreparedStatement pstmt = null;

		try {
			String sql = "SELECT channels, id FROM streams WHERE owner=? AND name=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, streamName);
			ResultSet rset = pstmt.executeQuery();
			rset.next();
			String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(1)));
			int id = rset.getInt(2);
			executeSqlInsertIntoTimeseries(id, strTuple, prefix);
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private void executeSqlInsertIntoTimeseries(int id, String strTuple, String prefix) throws SQLException {

		TimestampValues param = parseAddTupleRequestBody(strTuple, prefix);
		String format[] = prefix.split("_");

		if (format.length != param.values.size()) {
			throwInvalidTupleFormat(format);
		}

		String sql = "UPDATE " + prefix + "streams "
				+ "SET tuples = PutElem(tuples, "
				+ "row(?,";
		for (int i = 0; i < param.values.size(); i++) {
			sql += "?,";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")::" + prefix + "rowtype) WHERE id = ?";

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, param.timestamp);
			int i;
			for (i = 0; i < param.values.size(); i++) {
				if (format[i].equals("int")) {
					pstmt.setInt(i+2, (Integer)param.values.get(i));
				} else if (format[i].equals("float")) {
					pstmt.setDouble(i+2, (Double)param.values.get(i));
				} else if (format[i].equals("text")) {
					pstmt.setString(i+2, (String)param.values.get(i));
				} else {
					throwInvalidTupleFormat(format);
				}
			}
			pstmt.setInt(i + 2, id);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			if (e.toString().contains("Extra characters at the end of a datetime or interval."))
				throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
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

	private TimestampValues parseAddTupleRequestBody(String strTuple, String prefix) {
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
			throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
		}
		return timestamp;
	}

	private void throwInvalidTupleFormat(String[] format) {
		String tupleFormat = "[ " + VALID_TIMESTAMP_FORMAT + " (or null), ";
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

	private SqlBuilder convertCronTimeToSql(SqlBuilder sql) {
		Pattern cronExprPattern = Pattern.compile(CRON_TIME_REGULAR_EXPR);
		Matcher matcher = cronExprPattern.matcher(sql.getWhereCondition());
		while (matcher.find()) {
			String cronStr = matcher.group();
			String[] cronComponents = cronStr.split("[\\s\\[\\]]");
			List<String> cronComList = new ArrayList<String>();
			for (String str : cronComponents) {
				if (str.length() > 0) 
					cronComList.add(str);
			}
			if (cronComList.size() != 6) {
				throw new IllegalArgumentException(MSG_INVALID_CRON_EXPRESSION);
			}

			int i = 0;
			String sqlExpr = "( ";
			for (String cron : cronComList) {
				if (cron.equals("*")) {
					i++;
					continue;
				}
				String[] cronSplit;
				if (cron.contains("-")) {
					cronSplit = cron.split("-");
					if (cronSplit.length != 2) 
						throw new IllegalArgumentException(MSG_INVALID_CRON_EXPRESSION);
					sqlExpr += "( " + SQL_DATETIME_CONVERSIONS[i] + " >= " + cronSplit[0] + " AND " + SQL_DATETIME_CONVERSIONS[i] + " <= " + cronSplit[1] + " )";
				} else if (cron.contains(",")) {
					cronSplit = cron.split(",");
					sqlExpr += "( ";
					for (String number : cronSplit) {
						sqlExpr += SQL_DATETIME_CONVERSIONS[i] + " = " + number + " OR "; 
					}
					sqlExpr = sqlExpr.substring(0, sqlExpr.length() - 4) + " )";
				} else {
					sqlExpr += SQL_DATETIME_CONVERSIONS[i] + " = " + cron;
				}
				sqlExpr += " AND ";
				i++;
			}
			sqlExpr = sqlExpr.substring(0, sqlExpr.length() - 5);
			sqlExpr += " )";
			if (sql.condFilter != null) {
				sql.condFilter = sql.condFilter.replace(cronStr, sqlExpr);
			}
			if (sql.condRules != null) {
				sql.condRules = sql.condRules.replace(cronStr, sqlExpr);
			}
		}
		return sql;
	}

	private SqlBuilder convertDateTimePartExpression(SqlBuilder sql) {
		if (sql.condFilter != null) {
			sql.condFilter = sql.condFilter.replace("SECOND(timestamp)", DATETIME_SECOND);
			sql.condFilter = sql.condFilter.replace("second(timestamp)", DATETIME_SECOND);
			sql.condFilter = sql.condFilter.replace("MINUTE(timestamp)", DATETIME_MINUTE);
			sql.condFilter = sql.condFilter.replace("minute(timestamp)", DATETIME_MINUTE);
			sql.condFilter = sql.condFilter.replace("HOUR(timestamp)", DATETIME_HOUR);
			sql.condFilter = sql.condFilter.replace("hour(timestamp)", DATETIME_HOUR);
		}
		if (sql.condRules != null) {
			sql.condRules = sql.condRules.replace("SECOND(timestamp)", DATETIME_SECOND);
			sql.condRules = sql.condRules.replace("second(timestamp)", DATETIME_SECOND);
			sql.condRules = sql.condRules.replace("MINUTE(timestamp)", DATETIME_MINUTE);
			sql.condRules = sql.condRules.replace("minute(timestamp)", DATETIME_MINUTE);
			sql.condRules = sql.condRules.replace("HOUR(timestamp)", DATETIME_HOUR);
			sql.condRules = sql.condRules.replace("hour(timestamp)", DATETIME_HOUR);
		}
		return sql;
	}

	private SqlBuilder convertMacros(String owner, SqlBuilder sql) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			pstmt = conn.prepareStatement("SELECT macro_name, macro_value FROM macros WHERE owner = ?;");
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				String macroName = rset.getString(1);
				String macroValue = rset.getString(2);
				if (sql.condFilter != null) {
					sql.condFilter = sql.condFilter.replace("$(" + macroName + ")", macroValue);	
				}
				if (sql.condRules != null) {
					sql.condRules = sql.condRules.replace("$(" + macroName + ")", macroValue);	
				}
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}

		return sql;
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
			int offset) throws SQLException {

		// Check if stream name exists.
		if ( !isStreamNameExist(streamOwner, streamName) )
			throw new IllegalArgumentException("Stream (" + streamName + ") of owner (" + streamOwner + ") does not exists.");

		// Check timestamps
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
		Stream stream = getStreamInfo(streamOwner, streamName);

		// Build SQL
		SqlBuilder sql = new SqlBuilder(stream.id, offset, limit, getVirtualTableName(stream.channels), getStreamTableName(stream.channels), startTs, endTs, filter);

		// Apply rule condition.
		if (!streamOwner.equals(requestingUser)) {
			String ruleCond = getRuleCondition(streamOwner, requestingUser, stream);
			if (ruleCond == null) {
				// No allow rules for non-owner.
				return false;
			}
			sql.condRules = ruleCond;
		}

		// Process various eatures
		sql = convertMacros(streamOwner, sql);
		sql = convertCronTimeToSql(sql);
		sql = convertCStyleBooleanOperators(sql);
		sql = convertDateTimePartExpression(sql);		
		sql = convertChannelNames(stream.channels, sql);
		sql = processConditionOnOtherStreams(sql, streamOwner, stream);

		if (aggregator == null) {
			executeQuery(sql, stream);
		} else {
			if (filter == null && streamOwner.equals(requestingUser)) {
				// If aggregator with no filter, no rules, we can skip creating temporary filtered result.
				Aggregator agg = parseAggregator(aggregator);
				sql = processAggregateQueryNoFilter(agg, aggregator, sql, stream);
				sql.limit = limit;
				sql.offset = offset;
				executeQuery(sql, stream);
			} else {
				Aggregator agg = parseAggregator(aggregator);		
				sql = processAggregateQueryWithFilter(agg, aggregator, sql, stream);
				sql.limit = limit;
				sql.offset = offset;
				executeQuery(sql, stream);
			}
		}
		return true;
	}

	private String getVirtualTableName(List<Channel> channels) {
		return getChannelFormatPrefix(channels) + "vtable";
	}

	private void executeQuery(SqlBuilder sql, Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sqlStr = sql.buildSqlStatement(); 
			//Log.info(sqlStr);
			pstmt = sql.getPreparedStatement(conn, sqlStr);
			ResultSet rset = pstmt.executeQuery();

			storedPstmt = pstmt;
			storedResultSet = rset;
			storedStream = stream;
		} catch (SQLException e) {
			if (pstmt != null)
				pstmt.close();
			throw e;
		}
	}

	private class Aggregator {
		public static final String AGGREGATE_BY = "AggregateBy";
		public static final String AGGREGATE_RANGE = "AggregateRange";
		
		public String function;
		public String[] arguments;
	}
	
	private Aggregator parseAggregator(String expr) {
		//Log.info(expr);
		
		expr = expr.trim();
		if (expr.charAt(expr.length()-1) != ')') {
			throw new IllegalArgumentException(MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		
		Aggregator agg = new Aggregator();
		if (expr.toLowerCase().startsWith("aggregateby(")
				|| expr.startsWith("downsample(")) {
			agg.function = Aggregator.AGGREGATE_BY;
			expr = expr.toLowerCase().replace("aggregateby(", "").replace("downsample(", "");
			expr = expr.substring(0, expr.length() - 1);
		} else if (expr.toLowerCase().startsWith("aggregaterange(")
				|| expr.toLowerCase().startsWith("calculate(")) {
			agg.function = Aggregator.AGGREGATE_RANGE;
			expr = expr.toLowerCase().replace("aggregaterange(", "").replace("calculate(", "");
			expr = expr.substring(0, expr.length() - 1);
		} else {
			throw new IllegalArgumentException(MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		
		agg.arguments = expr.split(",");
		
		for (int i = 0; i < agg.arguments.length; i++) {
			agg.arguments[i] = agg.arguments[i].trim(); 
		}
		
		// Check argument validity
		if ( (agg.function == Aggregator.AGGREGATE_BY && agg.arguments.length != 2)
			|| (agg.function == Aggregator.AGGREGATE_RANGE && agg.arguments.length != 1)) {
			throw new IllegalArgumentException(MSG_INVALID_AGGREGATOR_EXPRESSION);
		}
		return agg;
	}
	
	private SqlBuilder processAggregateQueryNoFilter(Aggregator agg, String aggregator, SqlBuilder sql, Stream stream) throws SQLException {
		
		//Get row type for this stream
		String rowtype = getRowTypeName(stream.channels);

		// Get temporary UUID
		String tempName = newUUIDString();
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Create temporary tables
		createTempStreamTable(tempTable, rowtype);		
		createTempVirtualTable(tempVTable, tempTable);

		if (agg.function.equals(Aggregator.AGGREGATE_BY)) {
			// Check calendar and get valid calendar name
			String calendar = determineCalendar(agg.arguments[1]);
			String aggExpr = convertAggreagteExprChannelNames(stream.channels, agg.arguments[0]);
			
			// Generate AggregateBy SQL
			String aggregateBySql = "INSERT INTO " + tempTable 
					+ " SELECT " + stream.id 
					+ ", AggregateBy("
					+ "'" + aggExpr + "',"
					+ "'" + calendar + "',"
					+ "tuples,0";
			
			if (sql.startTime != null && sql.endTime != null) {
				aggregateBySql += ",'" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5),"
									+ "'" + sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5)";
			}
					
			aggregateBySql += ")::TimeSeries(" + rowtype + ") "
					+ "FROM " + sql.streamTableName + " WHERE id = " + stream.id + ";";

			//Log.info(aggregateBySql);

			// Execute AggregateBy
			PreparedStatement pstmt = null;
			try {
				pstmt = conn.prepareStatement(aggregateBySql);
				pstmt.executeUpdate();
			} finally {
				if (pstmt != null) {
					pstmt.close();
				}
			}

			// Modify original sql to refer to the temporary virtual table
			sql.virtualTableName = tempVTable;
			sql.streamTableName = tempTable;
			sql.removeTimeRange();

			return sql;
		} else if (agg.function.equals(Aggregator.AGGREGATE_RANGE)) {
			
			// Create an empty time seris for aggregate result.
			String insertSql = "INSERT INTO " + tempTable + " VALUES ( " + stream.id 
					+ ",'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";
			
			//Log.info(insertSql);
			
			PreparedStatement pstmt = null;
			try {
				pstmt = conn.prepareStatement(insertSql);
				pstmt.executeUpdate();
			} finally {
				if (pstmt != null) { 
					pstmt.close();
				}
			}
			
			String aggExpr = convertAggreagteExprChannelNames(stream.channels, agg.arguments[0]);
			
			String updateSql = 
					"UPDATE " + tempTable + " SET tuples = "
					+ "PutElem(tuples,("
						+ "SELECT AggregateRange('" + aggExpr + "',tuples,0";
			
			if (sql.startTime != null && sql.endTime != null) {
				updateSql += ",'" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5),'"
							+ sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5)";
			}
			updateSql += ")::" + rowtype 
						+ " FROM " + sql.streamTableName + " WHERE id = " + stream.id + "))"
						+ " WHERE id = " + stream.id + ";";
			
			//Log.info(updateSql);

			pstmt = null;
			try {
				pstmt = conn.prepareStatement(updateSql);
				pstmt.executeUpdate();
			} finally {
				if (pstmt != null) { 
					pstmt.close();
				}
			}

			// Modify original sql to refer to the temporary virtual table
			sql.virtualTableName = tempVTable;
			sql.streamTableName = tempTable;
			sql.removeTimeRange();

			return sql;
		} else {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	}

	private SqlBuilder processAggregateQueryWithFilter(Aggregator agg, String aggregator, SqlBuilder sql, Stream stream) throws SQLException {

		// Limit the duration.
		checkStartEndTimeDuration(sql);

		//Get row type for this stream
		String rowtype = getRowTypeName(stream.channels);

		// Get temporary UUID
		String tempName = newUUIDString();
		String tempTable = "streams_" + tempName;
		String tempVTable = "vtable_" + tempName;

		// Create temporary tables
		createTempStreamTable(tempTable, rowtype);		
		createTempVirtualTable(tempVTable, tempTable);

		// Create new empty timeseries
		String newTsSql = "INSERT INTO " + tempTable + " VALUES ( " + stream.id 
				+ ",'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";
		
		//Log.info(newTsSql);
		
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
		String insertSql = sql.buildSqlStatementInsertPrefix(tempVTable);
		
		//Log.info(insertSql);
		
		pstmt = null;
		try {
			pstmt = sql.getPreparedStatement(conn, insertSql);
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
		
		// Process aggregate function on the new time series.
		sql = processAggregateQueryNoFilter(agg, aggregator, sql, stream);
		
		return sql;
	}

	private String determineCalendar(String aggregatorOption) {
		if (aggregatorOption.equalsIgnoreCase("1min") 
				|| aggregatorOption.equalsIgnoreCase("min") 
				|| aggregatorOption.equalsIgnoreCase("minute") 
				|| aggregatorOption.equalsIgnoreCase("1minute") 
				|| aggregatorOption.equalsIgnoreCase("mins") 
				|| aggregatorOption.equalsIgnoreCase("minutes")) {
			return "ts_1min";
		} else if (aggregatorOption.equalsIgnoreCase("15min")
				|| aggregatorOption.equalsIgnoreCase("15mins")
				|| aggregatorOption.equalsIgnoreCase("15minutes")
				|| aggregatorOption.equalsIgnoreCase("15minute")) {
			return "ts_15min";
		} else if (aggregatorOption.equalsIgnoreCase("30min")
				|| aggregatorOption.equalsIgnoreCase("30mins")
				|| aggregatorOption.equalsIgnoreCase("30minutes")
				|| aggregatorOption.equalsIgnoreCase("30minute")) {
			return "ts_30min";
		} else if (aggregatorOption.equalsIgnoreCase("1hour") 
				|| aggregatorOption.equalsIgnoreCase("hour")
				|| aggregatorOption.equalsIgnoreCase("hours")) {
			return "ts_1hour";
		} else if (aggregatorOption.equalsIgnoreCase("1day") 
				|| aggregatorOption.equalsIgnoreCase("day")
				|| aggregatorOption.equalsIgnoreCase("days")) {
			return "ts_1day";
		} else if (aggregatorOption.equalsIgnoreCase("1week")
				|| aggregatorOption.equalsIgnoreCase("week")
				|| aggregatorOption.equalsIgnoreCase("weeks")) {
			return "ts_1week";
		} else if (aggregatorOption.equalsIgnoreCase("1month") 
				|| aggregatorOption.equalsIgnoreCase("month")
				|| aggregatorOption.equalsIgnoreCase("months")) {
			return "ts_1month";
		} else if (aggregatorOption.equalsIgnoreCase("1year") 
				|| aggregatorOption.equalsIgnoreCase("year")
				|| aggregatorOption.equalsIgnoreCase("years")) {
			return "ts_1year";
		} else {
			throw new IllegalArgumentException(MSG_INVALID_CALENDAR_TYPE);
		}
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
					throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
				}
			}
		}

		return dt;
	}

	private SqlBuilder processConditionOnOtherStreams(SqlBuilder sql, String streamOwner, Stream stream) throws SQLException {

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
				otherStreamMap.put(getStreamInfo(streamOwner, otherStream), channelSet);				
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
		int idx = stream.channels.size() + 1;
		for (Stream otherStream: otherStreamMap.keySet()) {
			for (Channel channel: otherStream.channels) {
				mergedChannel.add(channel);
				conversion.put(" " + otherStream.name + "." + channel.name + " ", " channel" + idx + " ");
				idx++;
			}
		}

		// Create row type for combined time series.
		String createRowtype = "CREATE ROW TYPE " + tempRowType + " ("
				+ "timestamp DATETIME YEAR TO FRACTION(5), ";
		idx = 1;
		for (Channel channel: stream.channels){
			if (channel.type.equals("float")) {
				createRowtype += "channel" + idx + " FLOAT, ";
			} else if (channel.type.equals("int")) {
				createRowtype += "channel" + idx + " INT, ";
			} else if (channel.type.equals("text")) {
				createRowtype += "channel" + idx + " VARCHAR(255), ";
			} else {
				throw new UnsupportedOperationException(MSG_UNSUPPORTED_TUPLE_FORMAT);
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
				throw new UnsupportedOperationException(MSG_UNSUPPORTED_TUPLE_FORMAT);
			}
			idx++;
		}
		createRowtype = createRowtype.substring(0, createRowtype.length() - 2) + ")";

		//Log.info(createRowtype);
		
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
		String executeUnion = "INSERT INTO " + tempTable + " SELECT " + stream.id + ", Union('" + sql.startTime.toString() + "'::DATETIME YEAR TO FRACTION(5), '" + sql.endTime.toString() + "'::DATETIME YEAR TO FRACTION(5), t1.tuples, ";

		for (idx = 2; idx <= otherStreamMap.keySet().size() + 1; idx++) {
			executeUnion += "t" + idx + ".tuples, ";
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 2) + ")::TimeSeries(" + tempRowType + ") FROM ";
		executeUnion += getStreamTableName(stream.channels) + " t1, "; 
		idx = 2;
		for (Stream otherStream: otherStreamMap.keySet()) {
			executeUnion += getStreamTableName(otherStream.channels) + " t" + idx + ", ";
			idx++;
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 2) + " WHERE ";
		executeUnion += "t1.id = " + stream.id + " AND ";
		idx = 2;
		for (Stream otherStream: otherStreamMap.keySet()) {
			executeUnion += "t" + idx + ".id = " + otherStream.id + " AND ";
			idx++;
		}
		executeUnion = executeUnion.substring(0, executeUnion.length() - 5) + ";";

		//Log.info(executeUnion);
		
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
			//Log.info(sql);
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
			//Log.info(sql);
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
	public JSONArray getNextJsonTuple() throws SQLException {
		if (storedResultSet == null) {
			cleanUpStoredInfo();
			return null;
		}

		if (storedResultSet.isClosed() || storedResultSet.isAfterLast()) {
			cleanUpStoredInfo();
			return null;
		}

		int i;
		if (storedResultSet.next()) {
			JSONArray curTuple = new JSONArray();
			curTuple.add(storedResultSet.getTimestamp(2).toString());
			i = 3;
			for (Channel channel: storedStream.channels) {
				if (channel.type.equals("float")) {
					curTuple.add(storedResultSet.getDouble(i));
				} else if (channel.type.equals("int")) {
					curTuple.add(storedResultSet.getInt(i));
				} else if (channel.type.equals("text")) {
					curTuple.add(storedResultSet.getString(i));
				} else {
					throw new UnsupportedOperationException(MSG_UNSUPPORTED_TUPLE_FORMAT);
				}
				i += 1;
			}
			return curTuple;
		} else {
			cleanUpStoredInfo();
			return null;
		}
	}

	private String getRuleCondition(String streamOwner, String requestingUser, Stream stream) throws SQLException {


		PreparedStatement pstmt = null;
		String ruleCond = null;
		try {
			String sql;

			if (requestingUser == null) {
				sql = "SELECT priority, condition, action "
						+ "FROM rules "
						+ "WHERE owner = ? "
						+ "AND ( ? IN target_streams OR target_streams IS NULL ) "
						+ "AND template_name IS NULL "
						+ "ORDER BY priority DESC;";
			} else {
				sql = "SELECT priority, condition, action "
						+ "FROM rules "
						+ "WHERE owner = ? "
						+ "AND ( ? IN target_streams OR target_streams IS NULL) "
						+ "AND ( ? IN target_users OR target_users IS NULL ) "
						+ "AND template_name IS NULL "
						+ "ORDER BY priority DESC;";
			}

			// Process allow rules			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, streamOwner);
			pstmt.setString(2, stream.name);
			if (requestingUser != null) {
				pstmt.setString(3, requestingUser);
			}

			ResultSet rset = pstmt.executeQuery();

			int prevPriority = -1;
			List<String> allowConds = new ArrayList<String>();
			List<String> denyConds = new ArrayList<String>();
			while (rset.next()) {
				int priority = rset.getInt(1);
				String condition = rset.getString(2);
				String action = rset.getString(3);

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
		if (!allowConds.isEmpty()) {
			if (ruleCond == null) { 
				ruleCond = "( " + StringUtils.join(allowConds, " OR ") + " )";
			} else {
				ruleCond = "( " + ruleCond + " ) OR ( " + StringUtils.join(allowConds, " OR ") + " )";
			}
		}
		if (!denyConds.isEmpty()) {
			if (ruleCond == null) {
				assert false;
			} else {
				ruleCond = "( " + ruleCond + " ) AND NOT ( " + StringUtils.join(denyConds, " OR ") + " )";
			}
		}
		return ruleCond;
	}

	private void addCurrentRule(List<String> allowConds, List<String> denyConds, String action, String condition) {
		if (action.equalsIgnoreCase("allow")) {
			allowConds.add("( " + condition + " )");
		} else if (action.equalsIgnoreCase("deny")) {
			denyConds.add("( " + condition + " )");
		}
	}

	private SqlBuilder convertCStyleBooleanOperators(SqlBuilder sql) {
		if (sql.condFilter != null) {
			sql.condFilter = sql.condFilter.replace("&&", " AND ");
			sql.condFilter = sql.condFilter.replace("||", " OR ");
		}
		if (sql.condRules != null) {
			sql.condRules = sql.condRules.replace("&&", " AND ");
			sql.condRules = sql.condRules.replace("||", " OR ");
		}

		return sql;
	}

	private String convertAggreagteExprChannelNames(List<Channel> channels, String expr) {
		Map<String, String> map = new HashMap<String, String>();
		int idx = 1;
		for (Channel ch: channels) {
			map.put("$" + ch.name, "$channel" + idx);
			idx += 1;
		}
		for (Map.Entry<String, String> item: map.entrySet()) {
			expr = expr.replace(item.getKey(), item.getValue());
		}
		return expr;
	}

	private String replaceStringNoDotPrefix(String expr, String from, String to) {
		String result = "";
		int idx;
		for (int curIdx = 0; curIdx >= 0; curIdx = idx + from.length()) {
			idx = expr.indexOf(from, curIdx);
			if (idx-1 >= 0) {
				if (expr.charAt(idx-1) == '.') {
					result += expr.substring(curIdx, idx);
					result += from;
					continue;
				}
			}				
			if (idx == curIdx) {				
				result += to;
			} else if (idx > curIdx) {
				result += expr.substring(curIdx, idx);
				result += to;
			} else {				
				result += expr.substring(curIdx, expr.length());
				break;
			}
		}
		return result;
	}
	
	private SqlBuilder convertChannelNames(List<Channel> channels, SqlBuilder sql) {
		Map<String, String> map = new HashMap<String, String>();
		int idx = 1;
		for (Channel ch: channels) {
			map.put(ch.name, "channel" + idx);
			idx += 1;
		}
		for (Map.Entry<String, String> item: map.entrySet()) {
			if (sql.condFilter != null) {
				sql.condFilter = replaceStringNoDotPrefix(sql.condFilter, item.getKey(), item.getValue());
			}
			if (sql.condRules != null) {
				sql.condRules = replaceStringNoDotPrefix(sql.condRules, item.getKey(), item.getValue());
			}
		}
		return sql;
	}

	@Override
	public Stream getStreamInfo(String owner, String streamName) throws SQLException {
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
			List<Channel> channels = getListChannelFromSqlArray(rset.getArray(2));
			int id = rset.getInt(3);
			pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + getStreamTableName(channels) + " WHERE id = ?");
			pstmt2.setInt(1, id);
			ResultSet rset2 = pstmt2.executeQuery();
			if (!rset2.next()) {
				throw new IllegalStateException("ResultSet is null.");
			}
			int numSamples = rset2.getInt(1);
			stream = new Stream(id, streamName, owner, tags, channels, numSamples);
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
		List<Stream> streams;
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		try {
			pstmt = conn.prepareStatement("SELECT id, name, tags, channels FROM streams WHERE owner = ?");
			pstmt.setString(1, owner);
			ResultSet rSet = pstmt.executeQuery();
			streams = new LinkedList<Stream>();
			while (rSet.next()) {
				int id = rSet.getInt(1);
				List<Channel> channels = getListChannelFromSqlArray(rSet.getArray(4));
				pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + getStreamTableName(channels) + " WHERE id = ?");
				pstmt2.setInt(1, id);
				ResultSet rset2 = pstmt2.executeQuery();
				if (!rset2.next()) {
					throw new IllegalStateException("ResultSet is null.");
				}
				int numSamples = rset2.getInt(1);
				streams.add(new Stream(id, rSet.getString(2), owner, rSet.getString(3), channels, numSamples)); 
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
			if (pstmt2 != null)
				pstmt2.close();
		}

		return streams;
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
			rset.next();
			int id = rset.getInt(1);
			String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(2)));

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
					throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
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
				String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(2)));
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

		throw new IllegalArgumentException("Unable to determine column delimiter.  Supported delimiters are tab or comma.");
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

	private File makeStringValidBulkloadFile(String data) throws IOException, NoSuchAlgorithmException {
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

	@Override
	public void bulkLoad(String owner, String streamName, String data) throws SQLException, IOException, NoSuchAlgorithmException {
		PreparedStatement pstmt = null;
		try {
			File file = makeStringValidBulkloadFile(data);
			Stream stream = getStreamInfo(owner, streamName);
			String prefix = getChannelFormatPrefix(stream.channels);
			
			pstmt = conn.prepareStatement("BEGIN WORK");
			pstmt.execute();
			pstmt.close();
			
			pstmt = conn.prepareStatement("UPDATE " + prefix + "streams SET tuples = BulkLoad(tuples, ?, ?) WHERE id = ?");
			pstmt.setString(1, file.getAbsolutePath());
			pstmt.setInt(2, TSOPEN_REDUCED_LOG);
			pstmt.setInt(3, stream.id);
			pstmt.executeUpdate();
			pstmt.close();
			
			pstmt = conn.prepareStatement("COMMIT WORK");
			pstmt.execute();
			
			file.delete();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	@Override
	public void queryStreamTest(
			String requestingUser,
			String streamOwner, 
			String streamName,
			String startTime, 
			String endTime, 
			String filter, 
			int limit,
			int offset) throws SQLException {

		// Check if stream name exists.
		if ( !isStreamNameExist(streamOwner, streamName) )
			throw new IllegalArgumentException("Stream name (" + streamName + ") does not exists.");

		PreparedStatement pstmt = null;
		//PreparedStatement countPstmt = null;
		try {
			Stream stream = getStreamInfo(streamOwner, streamName);
			String prefix = getChannelFormatPrefix(stream.channels);

			Timestamp startTs = null, endTs = null;
			try {
				if (startTime != null) 
					startTs = Timestamp.valueOf(startTime);
				if (endTime != null)
					endTs = Timestamp.valueOf(endTime);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
			}

			// Prepare sql statement.
			String sql = "SELECT Apply('$channel1, $channel2, $channel3', '" + filter + "', ?, ?, tuples)::TimeSeries(" + prefix + "rowtype) "
					+ "FROM " + prefix + "streams WHERE id = ?";

			//Log.info(sql);

			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, startTs);
			pstmt.setTimestamp(2, endTs);
			pstmt.setInt(3, stream.id);

			//Log.info("Executing query...");

			ResultSet rset;			
			rset = pstmt.executeQuery();

			//Log.info("Done.");

			if (rset.next()) {
				//Log.info("rset.getObject()");
				IfmxTimeSeries tseries = (IfmxTimeSeries) rset.getObject(1);
				//Log.info("Done.");
				while (tseries.next()) {
					//Log.info("result: " + tseries.getTimestamp(1) + ", " + tseries.getInt(2) + ", " + tseries.getInt(3) + ", " + tseries.getInt(4));
				}
			} else {
				//Log.info("rset.next() returned false.");
			}
			//Log.info("Done.");
		} catch (SQLException e) {
			if (pstmt != null)
				pstmt.close();
			throw e;
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
					String targetUsers = getSqlSetStringFromList(params.target_users);
					pstmt.setString(2, targetUsers);
				}
				if (params.target_streams == null) {
					pstmt.setArray(3,  templateTargetStreams);
				} else {
					String targetStreams = getSqlSetStringFromList(params.target_streams);
					pstmt.setString(3, targetStreams);
				}
				pstmt.setString(4, condition);
				pstmt.setString(5, action);
				pstmt.setInt(6, priority);
				pstmt.executeUpdate();
			} else {
				throw new IllegalArgumentException("Template (" + params.template_name + " doesn't exists.");						
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
}
