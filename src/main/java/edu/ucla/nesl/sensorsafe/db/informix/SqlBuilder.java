package edu.ucla.nesl.sensorsafe.db.informix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import edu.ucla.nesl.sensorsafe.model.Channel;
import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;

public class SqlBuilder {

	private static final String DATETIME_SECOND = "timestamp::DATETIME SECOND TO SECOND::CHAR(2)::INT";
	private static final String DATETIME_MINUTE = "timestamp::DATETIME MINUTE TO MINUTE::CHAR(2)::INT";
	private static final String DATETIME_HOUR = "timestamp::DATETIME HOUR TO HOUR::CHAR(2)::INT";
	private static final String DATETIME_DAY = "DAY(timestamp)";
	private static final String DATETIME_MONTH = "MONTH(timestamp)";
	private static final String DATETIME_WEEKDAY = "WEEKDAY(timestamp)";
	private static final String DATETIME_YEAR = "YEAR(timestamp)";
	private static final String CRON_TIME_REGULAR_EXPR = "\\[[\\d\\s-,\\*]+\\]";
	private static final String[] SQL_DATETIME_CONVERSIONS = { DATETIME_SECOND, DATETIME_MINUTE, DATETIME_HOUR, DATETIME_DAY, DATETIME_MONTH, DATETIME_WEEKDAY, DATETIME_YEAR };

	public String virtualTableName;
	public String streamTableName;
	public String condStreamID;
	public String condTimeRange;
	public String condFilter;
	public String condRules;
	
	public int offset;
	public int limit;
	public Timestamp startTime;
	public Timestamp endTime;
	
	public Stream stream;
	
	public SqlBuilder(int offset, int limit, Timestamp startTime, Timestamp endTime, String filter, Stream stream) {
		this.offset = offset;
		this.limit = limit;
		this.startTime = startTime;
		this.endTime = endTime;
		
		this.virtualTableName = stream.getVirtualTableName();
		this.streamTableName = stream.getStreamTableName();
		condStreamID = "id = ?";
		if (startTime != null && endTime != null) {
			condTimeRange = "timestamp BETWEEN ? AND ?";
		} else if (startTime == null && endTime != null) {
			condTimeRange = "timestamp <= ?";
		} else if (startTime != null && endTime == null) {
			condTimeRange = "timestamp >= ?";				
		}
		condFilter = filter;
		
		this.stream = stream;
	}

	private String joinString(String[] strs, String separator) {
		List<String> list = new ArrayList<String>();
		for (String str: strs) {
			if (str != null)
				list.add(str);
		}
		return StringUtils.join(list.toArray(), separator);
	}
	
	private String buildProjector(int offset, int limit) {
		String offsetStr = offset > 0 ? "SKIP ?" : null;
		String limitStr = limit > 0 ? "FIRST ?" : null;
		
		return joinString(new String[] { offsetStr, limitStr, "*" }, " ");
	}
	
	private String putParenthesis(String expr) {
		return expr == null ? null : "( " + expr + " )"; 
	}
	
	public String getWhereCondition() {
		return joinString(
				new String[] { 
					putParenthesis(condStreamID), 
					putParenthesis(condTimeRange), 
					putParenthesis(condFilter),
					putParenthesis(condRules)
				}, " AND ");
	}
	
	public String buildSqlStatement() {
		String where = getWhereCondition();
		String projection = buildProjector(offset, limit);
		if (where != null) {
			return joinString(new String[]{ "SELECT", projection, "FROM", virtualTableName, "WHERE", where}, " ");
		} else {
			return joinString(new String[]{ "SELECT", projection, "FROM", virtualTableName }, " ");
		}
	}

	public PreparedStatement getPreparedStatement(Connection conn, String sql) throws SQLException {
		PreparedStatement pstmt = null;
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
		if (condStreamID != null) {
			pstmt.setInt(i, stream.id);
			i += 1;
		}
		if (startTime != null) {
			pstmt.setTimestamp(i, startTime);
			i += 1;
		}
		if (endTime != null) {
			pstmt.setTimestamp(i, endTime);
			i += 1;
		}
		return pstmt;
	}

	public void removeTimeRange() {
		this.startTime = null;
		this.endTime = null;
		this.condTimeRange = null;
	}

	public void removeConditions() {
		condRules = null;
		condFilter = null;
	}

	public PreparedStatement getPreparedStatementInsertPrefix(Connection conn,	String table) throws SQLException {
		int tempOffset = offset;
		offset = 0;
		int tempLimit = limit;
		limit = 0;
		String sql = "INSERT INTO " + table + " " + buildSqlStatement();
		Log.info(sql);
		PreparedStatement pstmt = getPreparedStatement(conn, sql);
		offset = tempOffset;
		limit = tempLimit;
		return pstmt;
	}
	
	public static String getSqlSetString(Set<String> set) {
		String sqlSet = null;
		if (set != null) {
			sqlSet = "SET{";
			for (String item: set){
				sqlSet += "'" + item + "', ";
			}
			sqlSet = sqlSet.substring(0, sqlSet.length() - 2);
			sqlSet += "}";
		}
		return sqlSet;
	}

	public void makeValidSql(List<Macro> macros) throws SQLException {
		convertMacros(macros);
		convertCronTimeToSql();
		convertCStyleBooleanOperators();
		convertDateTimePartExpression();		
		convertChannelNames();
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
	
	private void convertChannelNames() {
		Map<String, String> map = new HashMap<String, String>();
		int idx = 1;
		for (Channel ch: stream.channels) {
			map.put(ch.name, "channel" + idx);
			idx += 1;
		}
		for (Map.Entry<String, String> item: map.entrySet()) {
			if (condFilter != null) {
				condFilter = replaceStringNoDotPrefix(condFilter, item.getKey(), item.getValue());
			}
			if (condRules != null) {
				condRules = replaceStringNoDotPrefix(condRules, item.getKey(), item.getValue());
			}
		}
	}

	private void convertCStyleBooleanOperators() {
		if (condFilter != null) {
			condFilter = condFilter.replace("&&", " AND ");
			condFilter = condFilter.replace("||", " OR ");
		}
		if (condRules != null) {
			condRules = condRules.replace("&&", " AND ");
			condRules = condRules.replace("||", " OR ");
		}
	}

	private void convertCronTimeToSql() {
		Pattern cronExprPattern = Pattern.compile(CRON_TIME_REGULAR_EXPR);
		Matcher matcher = cronExprPattern.matcher(getWhereCondition());
		while (matcher.find()) {
			String cronStr = matcher.group();
			String[] cronComponents = cronStr.split("[\\s\\[\\]]");
			List<String> cronComList = new ArrayList<String>();
			for (String str : cronComponents) {
				if (str.length() > 0) 
					cronComList.add(str);
			}
			if (cronComList.size() != 6) {
				throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_CRON_EXPRESSION);
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
						throw new IllegalArgumentException(ExceptionMessages.MSG_INVALID_CRON_EXPRESSION);
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
			if (condFilter != null) {
				condFilter = condFilter.replace(cronStr, sqlExpr);
			}
			if (condRules != null) {
				condRules = condRules.replace(cronStr, sqlExpr);
			}
		}
	}

	private void convertDateTimePartExpression() {
		if (condFilter != null) {
			condFilter = condFilter.replace("SECOND(timestamp)", DATETIME_SECOND);
			condFilter = condFilter.replace("second(timestamp)", DATETIME_SECOND);
			condFilter = condFilter.replace("MINUTE(timestamp)", DATETIME_MINUTE);
			condFilter = condFilter.replace("minute(timestamp)", DATETIME_MINUTE);
			condFilter = condFilter.replace("HOUR(timestamp)", DATETIME_HOUR);
			condFilter = condFilter.replace("hour(timestamp)", DATETIME_HOUR);
		}
		if (condRules != null) {
			condRules = condRules.replace("SECOND(timestamp)", DATETIME_SECOND);
			condRules = condRules.replace("second(timestamp)", DATETIME_SECOND);
			condRules = condRules.replace("MINUTE(timestamp)", DATETIME_MINUTE);
			condRules = condRules.replace("minute(timestamp)", DATETIME_MINUTE);
			condRules = condRules.replace("HOUR(timestamp)", DATETIME_HOUR);
			condRules = condRules.replace("hour(timestamp)", DATETIME_HOUR);
		}
	}

	private void convertMacros(List<Macro> macros) throws SQLException {
		for (Macro macro : macros) {
			if (condFilter != null) {
				condFilter = condFilter.replace("$(" + macro.name + ")", macro.value);	
			}
			if (condRules != null) {
				condRules = condRules.replace("$(" + macro.name + ")", macro.value);	
			}
		}
	}
}