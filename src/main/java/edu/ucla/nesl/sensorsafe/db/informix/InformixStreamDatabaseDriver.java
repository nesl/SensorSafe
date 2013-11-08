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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.naming.NamingException;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.informix.timeseries.IfmxTimeSeries;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.Stream;

public class InformixStreamDatabaseDriver extends InformixDatabaseDriver implements StreamDatabaseDriver {

	public InformixStreamDatabaseDriver() throws SQLException, IOException,
			NamingException, ClassNotFoundException {
		super();
	}

	private static final String ORIGIN_TIMESTAMP = "2000-01-01 00:00:00.00000";
	private static final String VALID_TIMESTAMP_FORMAT = "\"YYYY-MM-DD HH:MM:SS.[SSSSS]\"";
	
	private static final String MSG_INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format. Expected format is " + VALID_TIMESTAMP_FORMAT;
	private static final String MSG_INVALID_CRON_EXPRESSION = "Invalid cron expression. Expects [ sec(0-59) min(0-59) hour(0-23) day of month(1-31) month(1-12) day of week(0-6,Sun-Sat) ]";
	
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
	
	private PreparedStatement storedPstmt;
	private ResultSet storedResultSet;
	private Stream storedStream;

	static {
		try {
			initializeDatabase();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
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
					+ "UNIQUE (owner, name) CONSTRAINT owner_stream_name);");
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
					+ "target_users SET(VARCHAR(100) NOT NULL), "
					+ "target_streams SET(VARCHAR(100) NOT NULL), "
					+ "condition VARCHAR(255), "
					+ "action VARCHAR(255) NOT NULL);");
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
					+ "macro_value VARCHAR(255) NOT NULL);");
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

	@Override
	public List<Rule> getRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		List<Rule> rules = new ArrayList<Rule>();
		try {
			String sql = "SELECT id, target_users, target_streams, condition, action FROM rules WHERE owner = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				int id = rset.getInt(1);
				Array sqlArr = rset.getArray(2);
				Object[] targetUsers = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				sqlArr = rset.getArray(3);
				Object[] targetStreams = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				rules.add(new Rule(id, targetUsers, targetStreams, rset.getString(4), rset.getString(5)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return rules;
	}

	@Override
	public void addOrUpdateRule(String owner, Rule rule) throws SQLException {
		PreparedStatement pstmt = null;
		String targetUsers = null;
		if (rule.targetUsers != null) {
			targetUsers = "SET{";
			for (String user: rule.targetUsers){
				targetUsers += "'" + user + "', ";
			}
			targetUsers = targetUsers.substring(0, targetUsers.length() - 2);
			targetUsers += "}";
		}
		String targetStreams = null;
		if (rule.targetStreams != null) {
			targetStreams = "SET{";
			for (String stream: rule.targetStreams){
				targetStreams += "'" + stream + "', ";
			}
			targetStreams = targetStreams.substring(0, targetStreams.length() - 2);
			targetStreams += "}";
		}
		try {
			String sql;
			if (rule.id == 0) {
				sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action) VALUES (?,?,?,?,?)";
			} else {
				pstmt = conn.prepareStatement("SELECT 1 FROM rules WHERE id = ?");
				pstmt.setInt(1, rule.id);
				ResultSet rset = pstmt.executeQuery();
				if (rset.next()) {
					sql = "UPDATE rules SET owner = ?, "
							+ "target_users = ?, "
							+ "target_streams = ?, "
							+ "condition = ?, "
							+ "action = ? WHERE id = ?";
				} else {
					sql = "INSERT INTO rules (owner, target_users, target_streams, condition, action, id) VALUES (?,?,?,?,?,?)";
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
			if (rule.id != 0)
				pstmt.setInt(6, rule.id);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	@Override
	public void deleteAllRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("DELETE FROM rules WHERE owner = ?");
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
			pstmt = conn.prepareStatement("DELETE FROM rules WHERE owner = ? AND id = ?");
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
		String table = getTableName(channels);
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

	private String getTableName(List<Channel> channels) {
		String prefix = getChannelFormatPrefix(channels);
		return prefix + "streams";
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
			String table = getTableName(channels);
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

		AddTupleRequestParseResult param = parseAddTupleRequestBody(strTuple, prefix);

		String sql = "UPDATE " + prefix + "streams "
				+ "SET tuples = PutElem(tuples, "
				+ "row('" + param.timestamp.toString() + "', " + param.values + ")::" + prefix + "rowtype) "
				+ "WHERE id=?";

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			if (e.toString().contains("Extra characters at the end of a datetime or interval."))
				throw new IllegalArgumentException(MSG_INVALID_TIMESTAMP_FORMAT);
			else if (e.toString().contains("No cast from ROW")) 
				throwInvalidTupleFormat(prefix.split("_"));
			else 
				throw e;
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	private AddTupleRequestParseResult parseAddTupleRequestBody(String strTuple, String prefix) {
		Object objTuple = JSONValue.parse(strTuple);
		String values = "";
		Timestamp timestamp = null;
		String[] format = prefix.split("_");

		if (objTuple instanceof JSONArray) {
			boolean isFirst = true;
			int idx = 0;
			for (Object obj: (JSONArray)objTuple) {
				if (isFirst) {
					if (obj == null)
						timestamp = new Timestamp(System.currentTimeMillis());
					else {
						timestamp = parseTimestamp(obj);
					}
					isFirst = false;
				} else {
					values += parseTuple(format, idx, obj) + ", ";
					idx += 1;
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

			int idx = 0;
			for (Object obj: tuples) {
				values += parseTuple(format, idx, obj) + ", ";
				idx += 1;
			}
		} else {
			throwInvalidTupleFormat(format);
		}

		values = values.substring(0, values.length() - 2);

		return new AddTupleRequestParseResult(timestamp, values);
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

	private String parseTuple(String[] format, int idx, Object obj) {
		String value = "";
		try {
			if (format[idx].equals("float")) {
				if (obj instanceof Double)
					value = ((Double)obj).toString();
				else if (obj instanceof Integer)
					value = ((Integer)obj).toString();
				else 
					throw new ClassCastException();
			}							
			else if (format[idx].equals("int"))
				value = ((Integer)obj).toString();
			else if (format[idx].equals("text"))
				value = "'" + (String)obj + "'";
		} catch (ClassCastException | ArrayIndexOutOfBoundsException e) {
			throwInvalidTupleFormat(format);
		}

		return value;
	}


	private class AddTupleRequestParseResult {
		public Timestamp timestamp;
		public String values;

		public AddTupleRequestParseResult(Timestamp timestamp, String values) {
			this.timestamp = timestamp;
			this.values = values;
		}
	}

	private String processCronTimeExpression(String expr) {
		Pattern cronExprPattern = Pattern.compile("\\[[\\d\\s-,\\*]+\\]");
		Matcher matcher = cronExprPattern.matcher(expr);
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
			expr = expr.replace(cronStr, sqlExpr);
		}
		return expr;
	}
	
	private String convertDateTimePartExpression(String expr) {
		expr = expr.replace("SECOND(timestamp)", DATETIME_SECOND);
		expr = expr.replace("second(timestamp)", DATETIME_SECOND);
		expr = expr.replace("MINUTE(timestamp)", DATETIME_MINUTE);
		expr = expr.replace("minute(timestamp)", DATETIME_MINUTE);
		expr = expr.replace("HOUR(timestamp)", DATETIME_HOUR);
		expr = expr.replace("hour(timestamp)", DATETIME_HOUR);
		return expr;
	}
	
	private String convertMacros(String owner, String expr) throws SQLException {
		PreparedStatement pstmt = null;
		
		try {
			pstmt = conn.prepareStatement("SELECT macro_name, macro_value FROM macros WHERE owner = ?;");
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			while (rset.next()) {
				expr = expr.replace("$(" + rset.getString(1) + ")", rset.getString(2));
			}
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
		
		return expr;
	}

	public boolean prepareQuery(
			String requestingUser,
			String streamOwner, 
			String streamName, 
			String startTime, 
			String endTime, 
			String filter, 
			int limit, 
			int offset) 
				throws SQLException {
		
		// Check if stream name exists.
		if ( !isStreamNameExist(streamOwner, streamName) )
			throw new IllegalArgumentException("Stream (" + streamName + ") of owner (" + streamOwner + ") does not exists.");

		PreparedStatement pstmt = null;
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
			String sql = "SELECT";
			if (offset > 0) {
				sql += " SKIP ?";
			}
			if (limit > 0) {
				sql += " FIRST ?";
			}
			sql += " * FROM " + prefix + "vtable " + "WHERE id = ?"; 

			if (startTs != null) 
				sql += " AND timestamp >= ?";
			if (endTs != null)
				sql += " AND timestamp <= ?";
			if (filter != null) {
				sql += " AND ( " + filter + " )";
			}
			
			if (!streamOwner.equals(requestingUser)) {
				sql = applyRules(streamOwner, requestingUser, sql, stream);
				if (sql == null) {
					// No allow rules for non-owner.
					return false;
				}
			}
			sql = convertMacros(streamOwner, sql);
			sql = processCronTimeExpression(sql);
			sql = convertCStyleBooleanOperators(sql);
			sql = convertChannelNames(stream.channels, sql);
			sql = convertDateTimePartExpression(sql);
			
			//Log.info(sql + " (" + limit + ", " + stream.id + ")");
			
			pstmt = conn.prepareStatement(sql);
			int i = 1;
			if (offset > 0) {
				pstmt.setInt(i, offset);
				i+= 1;
			}
			if (limit > 0) {
				pstmt.setInt(i, limit);
				i += 1;
			}
			pstmt.setInt(i, stream.id);
			i += 1;
			if (startTs != null) {
				pstmt.setTimestamp(i, startTs);
				i += 1;
			}
			if (endTs != null) {
				pstmt.setTimestamp(i, endTs);
				i += 1;
			}

			ResultSet rset = pstmt.executeQuery();

			storedPstmt = pstmt;
			storedResultSet = rset;
			storedStream = stream;
		} catch (SQLException e) {
			if (pstmt != null)
				pstmt.close();
			throw e;
		}
		return true;
	}

	private void cleanUpStoredInfo() throws SQLException {
		if (storedPstmt != null) { 
			storedPstmt.close();
		}
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
					throw new UnsupportedOperationException("Unsupported tuple format.");
				}
				i += 1;
			}
			return curTuple;
		} else {
			cleanUpStoredInfo();
			return null;
		}
	}

	private String applyRules(String streamOwner, String requestingUser, String oriSql, Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		String ruleCond = null;
		try {
			String sql;
			String allowRuleCond = null;
			String denyRuleCond = null;

			if (requestingUser == null) 
				sql = "SELECT condition FROM rules WHERE owner = ? AND action = ? AND ( ? IN target_streams OR target_streams IS NULL )";
			else
				sql = "SELECT condition FROM rules WHERE owner = ? AND action = ? AND ( ? IN target_streams OR target_streams IS NULL) AND ( ? IN target_users OR target_users IS NULL )";

			// Process allow rules			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, streamOwner);
			pstmt.setString(2, "allow");
			pstmt.setString(3, stream.name);
			pstmt.setString(4, requestingUser);
			ResultSet rset = pstmt.executeQuery();
			List<String> sqlFragList = new LinkedList<String>();
			while (rset.next()) {
				String condition = rset.getString(1);				
				sqlFragList.add("( " + condition + " )"); 
			}
			
			if (sqlFragList.size() > 0) {
				allowRuleCond = "( " + StringUtils.join(sqlFragList, " OR ") + " )";
			} else {
				// No allow rules.
				return null;
			}
			
			// Process deny rules
			pstmt.setString(2, "deny");
			rset = pstmt.executeQuery();
			sqlFragList = new LinkedList<String>();
			while (rset.next()) {
				String condition = rset.getString(1);
				sqlFragList.add("( " + condition + " )"); 
			}
			if (sqlFragList.size() > 0)
				denyRuleCond = "NOT ( " + StringUtils.join(sqlFragList, " OR ") + " )";

			// Merge allow and deny rules.
			ruleCond = allowRuleCond;
			if (denyRuleCond != null) {
				ruleCond += " AND " + denyRuleCond;
			}
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
		return ruleCond != null ? oriSql + " AND " + ruleCond : oriSql;
	}

	private String convertCStyleBooleanOperators(String expr) {
		if (expr == null) 
			return null;

		expr = expr.replace("&&", " AND ");
		expr = expr.replace("||", " OR ");

		return expr;
	}

	private String convertChannelNames(List<Channel> channels, String expr) {
		if (expr == null)
			return null;

		//Generate map "name" -> "channel?"
		Map<String, String> map = new HashMap<String, String>();
		int idx = 1;
		for (Channel ch: channels) {
			map.put(ch.name, "channel" + idx);
			idx += 1;
		}
		for (Map.Entry<String, String> item: map.entrySet()) {
			expr = expr.replace(item.getKey(), item.getValue());
		}
		return expr;
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
			pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + getTableName(channels) + " WHERE id = ?");
			pstmt2.setInt(1, id);
			ResultSet rset2 = pstmt2.executeQuery();
			if (!rset2.next()) {
				throw new IllegalStateException("ResultSet is null.");
			}
			int numSamples = rset2.getInt(1);
			stream = new Stream(id, streamName, tags, channels, numSamples);
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
				pstmt2 = conn.prepareStatement("SELECT GetNelems(tuples) FROM " + getTableName(channels) + " WHERE id = ?");
				pstmt2.setInt(1, id);
				ResultSet rset2 = pstmt2.executeQuery();
				if (!rset2.next()) {
					throw new IllegalStateException("ResultSet is null.");
				}
				int numSamples = rset2.getInt(1);
				streams.add(new Stream(id, rSet.getString(2), rSet.getString(3), channels, numSamples)); 
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
	
	private String generateRandomKey() throws NoSuchAlgorithmException {
    	KeyGenerator keygenerator = KeyGenerator.getInstance("AES");
        SecretKey myDesKey = keygenerator.generateKey();
    	return myDesKey.getEncoded().toString().split("@")[1];
	}
	
	private File makeStringValidBulkloadFile(String data) throws IOException, NoSuchAlgorithmException {
		FileWriter fw = null;
		BufferedWriter bw = null;
		File file = null;
		
		try {
			String fileName = BULK_LOAD_DATA_FILE_NAME_PREFIX + generateRandomKey();
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
			pstmt = conn.prepareStatement("UPDATE " + prefix + "streams SET tuples = BulkLoad(tuples, ?) WHERE id = ?");
			pstmt.setString(1, file.getAbsolutePath());
			pstmt.setInt(2, stream.id);
			pstmt.executeUpdate();
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
}
