package edu.ucla.nesl.sensorsafe.informix;

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

import org.apache.commons.lang.StringUtils;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.RuleCollection;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

public class InformixStreamDatabaseDriver extends InformixDatabaseDriver implements StreamDatabaseDriver {

	private static final String ORIGIN_TIMESTAMP = "2000-01-01 00:00:00.00000";
	private static final String VALID_TIMESTAMP_FORMAT = "\"YYYY-MM-DD HH:MM:SS.[SSSSS]\"";
	private static final String MSG_INVALID_TIMESTAMP_FORMAT = "Invalid timestamp format. Expected format is " + VALID_TIMESTAMP_FORMAT;

	private static InformixStreamDatabaseDriver instance;

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
		Statement stmt2 = null;
		try {
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();

			// clean up
			ResultSet rset = stmt.executeQuery("SELECT 1 FROM systables WHERE tabname='streams';");
			if (rset.next()) {
				stmt.close();
				stmt = conn.createStatement();
				rset = stmt.executeQuery("SELECT channels FROM streams;");
				while (rset.next()) {
					String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(1)));
					stmt2.execute("DROP TABLE IF EXISTS " + prefix + "vtable");
					stmt2.execute("DROP TABLE IF EXISTS " + prefix + "streams");
					stmt2.execute("DROP ROW TYPE IF EXISTS " + prefix + "rowtype RESTRICT");
				}
				if (stmt != null)
					stmt.close();
			}

			stmt = conn.createStatement();
			stmt.execute("DROP TABLE IF EXISTS streams;");
			stmt.execute("DROP TABLE IF EXISTS rules;");

			stmt.execute("DELETE FROM CalendarPatterns WHERE cp_name = 'every_sec';");
			stmt.execute("DELETE FROM CalendarTable WHERE c_name = 'sec_cal';");

			// calendars
			stmt.execute("INSERT INTO CalendarPatterns VALUES('every_sec', '{1 on}, second');");
			stmt.execute("INSERT INTO CalendarTable(c_name, c_calendar) VALUES ('sec_cal', 'startdate(1753-01-01 00:00:00.00000), pattname(every_sec)');");

			// row types
			stmt.execute("DROP ROW TYPE IF EXISTS name_type RESTRICT");

			// tables
			stmt.execute("CREATE TABLE streams ("
					+ "id BIGINT NOT NULL PRIMARY KEY, "
					+ "name VARCHAR(255) UNIQUE CONSTRAINT name, "
					+ "tags VARCHAR(255), "
					+ "channels LIST(ROW(name VARCHAR(255), type VARCHAR(255)) NOT NULL));");

			stmt.execute("CREATE TABLE rules ("
					+ "target_users SET(VARCHAR(255) NOT NULL), "
					+ "target_streams SET(VARCHAR(255) NOT NULL), "
					+ "condition VARCHAR(255) NOT NULL, "
					+ "action VARCHAR(255) NOT NULL);");

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
	public RuleCollection getRules() throws SQLException {
		PreparedStatement pstmt = null;
		RuleCollection rules = new RuleCollection();
		try {
			String sql = "SELECT * FROM rules";
			pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			rules.rules = new LinkedList<Rule>();
			while (rset.next()) {
				Array sqlArr = rset.getArray(1);
				Object[] targetUsers = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				sqlArr = rset.getArray(2);
				Object[] targetStreams = sqlArr != null ? (Object[])sqlArr.getArray() : null;
				rules.rules.add(new Rule(targetUsers, targetStreams, rset.getString(3), rset.getString(4)));
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
		return rules;
	}

	@Override
	public void storeRule(Rule rule) throws SQLException {
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
			String sql = "INSERT INTO rules VALUES (?,?,?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, targetUsers);
			pstmt.setString(2, targetStreams);
			pstmt.setString(3, rule.condition);
			pstmt.setString(4, rule.action);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null) 
				pstmt.close();
		}
	}

	@Override
	public void deleteAllRules() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute("DELETE FROM rules");
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Override
	public void createStream(Stream stream) throws SQLException, ClassNotFoundException {

		// Check if tupleFormat is valid
		if (!isChannelsValid(stream.channels))
			throw WebExceptionBuilder.buildBadRequest("Invalid channel definition.");

		// Check if stream name exists.
		if ( isStreamNameExist(stream.name) )
			throw WebExceptionBuilder.buildBadRequest("Stream name (" + stream.name + ") already exists.");

		// Get new stream id number.
		int id = getNewStreamId();

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
		executeSqlInsertIntoStreamTable(id, stream);

		// Insert into timeseries streams table
		executeSqlInsertIntoTimeseriesTable(id, stream.channels);

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

	private boolean isStreamNameExist(String newStreamName) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "SELECT COUNT(name) FROM streams WHERE name=?;";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newStreamName);
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

	private int getNewStreamId() throws SQLException {
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
	}

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

			Log.info(sql);

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

	private void executeSqlInsertIntoStreamTable(int id, Stream stream) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement("INSERT INTO streams VALUES (?,?,?,?)");
			pstmt.setInt(1, id);
			pstmt.setString(2, stream.name);
			pstmt.setString(3, stream.tags);
			pstmt.setString(4, getChannelsSqlDataString(stream.channels));
			pstmt.executeUpdate(); 
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

	private void executeSqlInsertIntoTimeseriesTable(int id, List<Channel> channels) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			String table = getTableName(channels);
			String sql = "INSERT INTO " + table + " VALUES (?, "
					+ "'origin(" + ORIGIN_TIMESTAMP + "),calendar(sec_cal),threshold(0),irregular,[]');";
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	private void executeSqlCreateVirtualTable(Stream stream) throws SQLException {
		Statement stmt = null;

		try {
			stmt = conn.createStatement();
			String prefix = getChannelFormatPrefix(stream.channels);
			String vtableName = prefix + "vtable";
			String sTableName = prefix + "streams";
			try {
				stmt.execute("EXECUTE PROCEDURE TSCreateVirtualTAB('" + vtableName + "', '" + sTableName + "')");
			} catch (SQLException e) {
				if (!e.toString().contains("already exists in database."))
					throw e;
			}
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Override
	public void addTuple(String name, String strTuple) throws SQLException {
		// Check if stream name exists.
		if ( !isStreamNameExist(name) )
			throw WebExceptionBuilder.buildBadRequest("Stream name (" + name + ") does not exists.");

		PreparedStatement pstmt = null;

		try {
			String sql = "SELECT channels, id FROM streams WHERE name=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, name);
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

		Log.info(sql);

		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			if (e.toString().contains("Extra characters at the end of a datetime or interval."))
				throw WebExceptionBuilder.buildBadRequest(MSG_INVALID_TIMESTAMP_FORMAT);
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
			throw WebExceptionBuilder.buildBadRequest(MSG_INVALID_TIMESTAMP_FORMAT);
		}
		return timestamp;
	}

	private void throwInvalidTupleFormat(String[] format) {
		String tupleFormat = "[ " + VALID_TIMESTAMP_FORMAT + " (or null), ";
		for (String strFormat: format) {
			tupleFormat += strFormat + ", ";
		}
		tupleFormat = tupleFormat.substring(0, tupleFormat.length() - 2) + " ]";
		throw WebExceptionBuilder.buildBadRequest("Invalid tuple data type. Expected tuple format: " + tupleFormat);
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
	public JSONObject queryStream(String name, String startTime, String endTime, String expr) throws SQLException, JsonProcessingException {
		// Check if stream name exists.
		if ( !isStreamNameExist(name) )
			throw WebExceptionBuilder.buildBadRequest("Stream name (" + name + ") does not exists.");

		PreparedStatement pstmt = null;
		JSONObject json = null;
		try {
			Stream stream = getStreamInfo(name);
			String prefix = getChannelFormatPrefix(stream.channels);

			Timestamp startTs = null, endTs = null;
			try {
				if (startTime != null) 
					startTs = Timestamp.valueOf(startTime);
				if (endTime != null)
					endTs = Timestamp.valueOf(endTime);
			} catch (IllegalArgumentException e) {
				throw WebExceptionBuilder.buildBadRequest(MSG_INVALID_TIMESTAMP_FORMAT);
			}

			String sql = "SELECT * FROM " + prefix + "vtable "
					+ "WHERE id=?";

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
			pstmt.setInt(1, stream.id);
			if (startTs != null)
				pstmt.setTimestamp(2, startTs);
			if (endTs != null)
				pstmt.setTimestamp(3, endTs);

			ResultSet rset = pstmt.executeQuery();	

			ObjectMapper mapper = new ObjectMapper();
			AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
			mapper.setAnnotationIntrospector(introspector);
			json = (JSONObject)JSONValue.parse(mapper.writeValueAsString(stream));

			JSONArray tuples = new JSONArray();

			while (rset.next()) {
				JSONArray curTuple = new JSONArray();
				curTuple.add(rset.getTimestamp(2).toString());
				int i = 3;
				for (Channel channel: stream.channels) {
					if (channel.type.equals("float")) {
						curTuple.add(rset.getDouble(i));
					} else if (channel.type.equals("int")) {
						curTuple.add(rset.getInt(i));
					} else if (channel.type.equals("text")) {
						curTuple.add(rset.getString(i));
					} else {
						throw WebExceptionBuilder.buildInternalServerError("Unsupported tuple format.");
					}
					i += 1;
				}
				tuples.add(curTuple);
			}

			json.put("tuples", tuples);
		} finally {
			if (pstmt != null)
				pstmt.close();
		}

		return json;
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
	public Stream getStreamInfo(String name) throws SQLException {
		PreparedStatement pstmt = null;
		Stream stream = null;
		try {
			String sql = "SELECT tags, channels, id FROM streams WHERE name=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, name);
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
	public List<Stream> getStreamList() throws SQLException {
		List<Stream> streams;
		Statement stmt = null;

		try {
			stmt = conn.createStatement();
			ResultSet rSet = stmt.executeQuery("SELECT id, name, tags, channels FROM streams");
			streams = new LinkedList<Stream>();
			while (rSet.next()) {
				streams.add(new Stream(rSet.getInt(1), rSet.getString(2), rSet.getString(3), getListChannelFromSqlArray(rSet.getArray(4)))); 
			}
		} finally {
			if (stmt != null)
				stmt.close();
		}

		return streams;
	}

	@Override
	public void deleteStream(String name, String startTime, String endTime) throws SQLException {
		PreparedStatement pstmt = null;

		try {
			// Get stream information.
			String sql = "SELECT id, channels FROM streams WHERE name=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, name);
			ResultSet rset = pstmt.executeQuery();
			rset.next();
			int id = rset.getInt(1);
			String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(2)));

			if (pstmt != null) {
				pstmt.close();
			}

			// Delete a stream.
			if (startTime == null && endTime == null) {
				sql = "DELETE FROM streams WHERE id=?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, id);
				pstmt.executeUpdate();

				if (pstmt != null)
					pstmt.close();
				sql = "DELETE FROM " + prefix + "_streams WHERE id=?";
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
					throw WebExceptionBuilder.buildBadRequest(MSG_INVALID_TIMESTAMP_FORMAT);
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
						+ "WHERE id=?";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, id);
				pstmt.executeUpdate();
			} else {
				throw WebExceptionBuilder.buildBadRequest("Both the query parameters start_time and end_time or none of them should be provided.");
			}
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	@Override
	public void deleteAllStreams() throws SQLException {
		Statement stmt = null;
		Statement stmt2 = null;
		try {
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rset = stmt.executeQuery("SELECT channels FROM streams");
			while (rset.next()) {
				String prefix = getChannelFormatPrefix(getListChannelFromSqlArray(rset.getArray(1)));
				stmt2.execute("DROP TABLE IF EXISTS " + prefix + "vtable");
				stmt2.execute("DROP TABLE IF EXISTS " + prefix + "streams");
			}
			stmt2.execute("DELETE FROM streams");
		} finally {
			if (stmt != null)
				stmt.close();
			if (stmt2 != null)
				stmt.close();
		}
	}

	@Override
	public void setCurrentUser(String username) {
		// TODO
	}
}
