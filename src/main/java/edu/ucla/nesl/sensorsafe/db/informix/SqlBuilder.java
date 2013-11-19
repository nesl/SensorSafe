package edu.ucla.nesl.sensorsafe.db.informix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SqlBuilder {
	public String virtualTableName;
	public String streamTableName;
	public String condStreamID;
	public String condTimeRange;
	public String condFilter;
	public String condRules;
	
	public int streamID;
	public int offset;
	public int limit;
	public Timestamp startTime;
	public Timestamp endTime;
	
	public SqlBuilder(int streamID, int offset, int limit, String virtualTableName, String streamTableName, Timestamp startTime, Timestamp endTime, String filter) {
		this.streamID = streamID;
		this.offset = offset;
		this.limit = limit;
		this.startTime = startTime;
		this.endTime = endTime;
		
		this.virtualTableName = virtualTableName;
		this.streamTableName = streamTableName;
		condStreamID = "id = ?";
		if (startTime != null && endTime != null) {
			condTimeRange = "timestamp BETWEEN ? AND ?";
		} else if (startTime == null && endTime != null) {
			condTimeRange = "timestamp <= ?";
		} else if (startTime != null && endTime == null) {
			condTimeRange = "timestamp >= ?";				
		}
		condFilter = filter;
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
			pstmt.setInt(i, streamID);
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

	public String buildSqlStatementInsertPrefix(String table) {
		offset = 0;
		limit = 0;
		return "INSERT INTO " + table + " " + buildSqlStatement();
	}

	public void removeConditions() {
		condRules = null;
		condFilter = null;
	}
}