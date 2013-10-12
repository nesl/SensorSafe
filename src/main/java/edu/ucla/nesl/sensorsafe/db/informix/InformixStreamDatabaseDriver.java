package edu.ucla.nesl.sensorsafe.db.informix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.lang.StringUtils;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.RuleCollection;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class InformixStreamDatabaseDriver extends InformixDatabaseDriver implements StreamDatabaseDriver {

	private static final String ORIGIN_TIMESTAMP = "2000-01-01 00:00:00.00000";
	private static final String VALID_TIMESTAMP_FORMAT = "\"YYYY-MM-DD HH:MM:SS.[SSSSS]\"";
	private static final String MSG_INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format. Expected format is " + VALID_TIMESTAMP_FORMAT;

	private static final String BULK_LOAD_DATA_FILE_NAME = "tmp/bulkload_data";

	private static InformixStreamDatabaseDriver instance;

	private PreparedStatement storedPstmt;
	private ResultSet storedResultSet;
	private Stream storedStream;

	public static InformixStreamDatabaseDriver getInstance() {
		if (instance == null) {
			instance = new InformixStreamDatabaseDriver();
		}
		return instance;
	}

	private InformixStreamDatabaseDriver() {
		// TODO Load database configuration properties here.
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

	@Override
	protected void initializeDatabase() throws SQLException, ClassNotFoundException {
		PreparedStatement pstmt = null;
		try {
			// Check if tables are there in database.
			String sql = "SELECT 1 FROM systables WHERE tabname=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "streams");
			ResultSet rset = pstmt.executeQuery();
			if (!rset.next())
				initializeTables();

			// Add type map information
			Map<String, Class<?>> typeMap = conn.getTypeMap();
			typeMap.put("calendarpattern", Class.forName("com.informix.timeseries.IfmxCalendarPattern"));
			typeMap.put("calendar", Class.forName("com.informix.timeseries.IfmxCalendar"));
			conn.setTypeMap(typeMap);
		} finally {
			if (pstmt != null)
				pstmt.close();
		}

	}

	private void initializeTables() throws SQLException, ClassNotFoundException {

		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			// calendars
			stmt.execute("DELETE FROM CalendarPatterns WHERE cp_name = 'every_sec';");
			stmt.execute("DELETE FROM CalendarTable WHERE c_name = 'sec_cal';");
			stmt.execute("INSERT INTO CalendarPatterns VALUES('every_sec', '{1 on}, second');");
			stmt.execute("INSERT INTO CalendarTable(c_name, c_calendar) VALUES ('sec_cal', 'startdate(1753-01-01 00:00:00.00000), pattname(every_sec)');");

			// tables
			stmt.execute("CREATE TABLE streams ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "name VARCHAR(100) NOT NULL, "
					+ "tags VARCHAR(255), "
					+ "channels LIST(ROW(name VARCHAR(100),type VARCHAR(100)) NOT NULL), "
					+ "UNIQUE (owner, name) CONSTRAINT owner_stream_name);");

			String sql = "CREATE TABLE rules ("
					+ "id SERIAL PRIMARY KEY NOT NULL, "
					+ "owner VARCHAR(100) NOT NULL, "
					+ "target_users SET(VARCHAR(100) NOT NULL), "
					+ "target_streams SET(VARCHAR(100) NOT NULL), "
					+ "condition VARCHAR(255), "
					+ "action VARCHAR(255) NOT NULL);";
			stmt.execute(sql);

			// Test
			/*stmt.executeUpdate("INSERT INTO streams VALUES (1, 'test', 'tags', LIST{ROW('acc_x', 'float'), ROW('acc_y', 'float')});");

            ResultSet rset = stmt.executeQuery("SELECT channels FROM streams");
            rset.next();
            Array sqlArr = rset.getArray(1);
            Object[] arr = (Object[])sqlArr.getArray();
            for (Object obj: arr) {
            	Struct struct = (Struct)obj;
            	Object[] attr = struct.getAttributes();
            	Log.info((String)attr[0]);
            	Log.info((String)attr[1]);
            }*/
		} 
		finally {
			if (stmt != null)
				stmt.close();
		}
	}

	private List<Channel> getListChannelFromSqlArray(Array sqlArray) throws SQLException {
		List<Channel> channels = new LinkedList<Channel>();
		Object[] objArray = (Object[])sqlArray.getArray();
		for (Object obj: objArray) {
			Struct struct = (Struct)obj;
			Object[] attr = struct.getAttributes();
			channels.add(new Channel((String)attr[0], (String)attr[1]));
		}
		return channels;
	}

	private String getChannelFormatPrefix(List<Channel> channels) {
		String prefix = "";
		for (Channel channel: channels) {
			prefix += channel.type + "_";
		}
		return prefix;
	}

	@Override
	public RuleCollection getRules(String owner) throws SQLException {
		PreparedStatement pstmt = null;
		RuleCollection rules = new RuleCollection();
		try {
			String sql = "SELECT id, target_users, target_streams, condition, action FROM rules WHERE owner = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			ResultSet rset = pstmt.executeQuery();
			rules.rules = new LinkedList<Rule>();
			while (rset.next()) {
				int id = rset.getInt(1);
				Array sqlArr = rset.getArray(2);
				Object[] targetUsers = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				sqlArr = rset.getArray(3);
				Object[] targetStreams = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				rules.rules.add(new Rule(id, targetUsers, targetStreams, rset.getString(4), rset.getString(5)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return rules;
	}

	@Override
	public void storeRule(String owner, Rule rule) throws SQLException {
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

	private String getRowTypeName(List<Channel> channels) {
		return getChannelFormatPrefix(channels) + "rowtype";
	}

	private void executeSqlCreateStreamTable(List<Channel> channels) throws SQLException {
		String rowtype = getRowTypeName(channels);
		String table = getTableName(channels);
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "CREATE TABLE IF NOT EXISTS " + table 
					+ " (id BIGINT NOT NULL PRIMARY KEY, tuples TIMESERIES(" + rowtype + "))";
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

	@Override
	public void prepareQueryStream(String owner, 
			String streamName, 
			String startTime, 
			String endTime, 
			String expr, 
			int limit, int offset) 
					throws SQLException {
		// Check if stream name exists.
		if ( !isStreamNameExist(owner, streamName) )
			throw new IllegalArgumentException("Stream name (" + streamName + ") does not exists.");

		PreparedStatement pstmt = null;
		try {
			Stream stream = getStreamInfo(owner, streamName);
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
			if (offset != 0) {
				sql += " SKIP ?";
			}
			if (limit != 0) {
				sql += " FIRST ?";
			}
			sql += " * FROM " + prefix + "vtable " + "WHERE id = ?"; 

			if (startTs != null) 
				sql += " AND timestamp>=?";
			if (endTs != null)
				sql += " AND timestamp<=?";
			if (expr != null) 
				sql += " AND ( " + expr + " )";

			sql = applyRules(null, sql, stream);

			sql = convertCStyleBooleanOperators(sql);
			sql = convertChannelNames(stream.channels, sql);

			Log.info(sql);

			pstmt = conn.prepareStatement(sql);			
			int i = 1;
			if (offset != 0) {
				pstmt.setInt(i, offset);
				i+= 1;
			}
			if (limit != 0) {
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

			Log.info("executeQuery()");
			ResultSet rset = pstmt.executeQuery();
			Log.info("executeQuery() Done.");

			storedPstmt = pstmt;
			storedResultSet = rset;
			storedStream = stream;
		} catch (SQLException e) {
			if (pstmt != null)
				pstmt.close();
			throw e;
		}
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
	
	private String applyRules(String user, String oriSql, Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		String ruleCond = null;
		try {
			String sql;
			String allowRuleCond = null;
			String denyRuleCond = null;

			if (user == null) 
				sql = "SELECT * FROM rules WHERE action = ? AND ? IN target_streams OR target_streams IS NULL";
			else
				sql = "SELECT * FROM rules WHERE action = ? AND (? IN target_streams OR target_streams IS NULL) AND (? IN target_users OR target_users IS NULL)";

			// Process allow rules			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "allow");
			pstmt.setString(2, stream.name);
			if (user != null)
				pstmt.setString(3, user);
			ResultSet rset = pstmt.executeQuery();
			List<String> sqlFragList = new LinkedList<String>();
			while (rset.next()) {
				String condition = rset.getString(3);
				sqlFragList.add("( " + condition + " )"); 
			}
			if (sqlFragList.size() > 0)
				allowRuleCond = "( " + StringUtils.join(sqlFragList, " OR ") + " )";

			if (pstmt != null) 
				pstmt.close();

			// Process deny rules
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "deny");
			pstmt.setString(2, stream.name);
			if (user != null)
				pstmt.setString(3, user);
			rset = pstmt.executeQuery();
			sqlFragList = new LinkedList<String>();
			while (rset.next()) {
				String condition = rset.getString(3);
				sqlFragList.add("( " + condition + " )"); 
			}
			if (sqlFragList.size() > 0)
				denyRuleCond = "NOT ( " + StringUtils.join(sqlFragList, " OR ") + " )";

			if (allowRuleCond != null && denyRuleCond != null) 
				ruleCond = allowRuleCond + " AND " + denyRuleCond;
			else if (allowRuleCond == null && denyRuleCond != null)
				ruleCond = denyRuleCond;
			else if (allowRuleCond != null && denyRuleCond == null)
				ruleCond = allowRuleCond;

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
	public Stream getStreamInfo(String owner, String name) throws SQLException {
		PreparedStatement pstmt = null;
		Stream stream = null;
		try {
			String sql = "SELECT tags, channels, id FROM streams WHERE owner = ? AND name = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, owner);
			pstmt.setString(2, name);
			ResultSet rset = pstmt.executeQuery();
			rset.next();
			String tags = rset.getString(1);
			List<Channel> channels = getListChannelFromSqlArray(rset.getArray(2));
			int id = rset.getInt(3);
			stream = new Stream(id, name, tags, channels);
		} finally {
			if (pstmt != null)
				pstmt.close();
		}		

		return stream;
	}

	@Override
	public List<Stream> getStreamList(String owner) throws SQLException {
		List<Stream> streams;
		PreparedStatement pstmt = null;

		try {
			pstmt = conn.prepareStatement("SELECT id, name, tags, channels FROM streams WHERE owner = ?");
			pstmt.setString(1, owner);
			ResultSet rSet = pstmt.executeQuery();
			streams = new LinkedList<Stream>();
			while (rSet.next()) {
				streams.add(new Stream(rSet.getInt(1), rSet.getString(2), rSet.getString(3), getListChannelFromSqlArray(rSet.getArray(4)))); 
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
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

				String strStartTs = null, strEndTs = null;
				if (startTs != null) {
					strStartTs = "'" + startTs.toString() + "'::DATETIME YEAR TO FRACTION(5)"; 
				}
				if (endTs != null) {
					strEndTs = "'" + endTs.toString() + "'::DATETIME YEAR TO FRACTION(5)";
				}

				sql = "UPDATE " + prefix + "streams SET tuples = "
						+ "DelRange(tuples, " + strStartTs + ", " + strEndTs + ") "
						+ "WHERE id = ?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, id);
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

	@Override
	public void bulkLoad(String owner, String streamName, String data) throws SQLException, IOException {
		PreparedStatement pstmt = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			File file = new File(BULK_LOAD_DATA_FILE_NAME);
			if (file.exists()) {
				file.delete();				
			}
			file.createNewFile();
			fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			bw.write(data);
			bw.close();

			Stream stream = getStreamInfo(owner, streamName);
			String prefix = getChannelFormatPrefix(stream.channels);
			pstmt = conn.prepareStatement("UPDATE " + prefix + "streams SET tuples = BulkLoad(tuples, ?)");
			pstmt.setString(1, file.getAbsolutePath());
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
			if (fw != null)
				fw.close();
			if (bw != null)
				bw.close();
		}
	}


}
