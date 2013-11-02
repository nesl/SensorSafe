package edu.ucla.nesl.sensorsafe.db;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;

import net.minidev.json.JSONArray;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.Stream;

public interface StreamDatabaseDriver extends DatabaseDriver {
	
	public List<Rule> getRules(String owner) 
			throws SQLException;
	
	public void addOrUpdateRule(String owner, Rule rule) 
			throws SQLException;
	
	public void deleteAllRules(String owner) 
			throws SQLException;
	
	public void createStream(String owner, Stream stream) 
			throws SQLException, ClassNotFoundException;
	
	public void addTuple(String owner, String streamName, String strTuple) 
			throws SQLException;
	
	public void prepareQuery(String requestingUser, String streamOwner, String streamName, String startTime, String endTime, String expr, int limit, int offset) 
			throws SQLException, JsonProcessingException;
	
	public Stream getStreamInfo(String owner, String name) 
			throws SQLException;
	
	public List<Stream> getStreamList(String owner) 
			throws SQLException;

	public void deleteStream(String owner, String streamName, String startTime, String endTime)
			throws SQLException;
	
	public void deleteAllStreams(String owner) 
			throws SQLException;

	public void clean() throws SQLException, ClassNotFoundException;

	public void deleteRule(String remoteUser, int id) throws SQLException;

	public void bulkLoad(String owner, String streamName, String data) throws SQLException, IOException, NoSuchAlgorithmException;

	public JSONArray getNextJsonTuple() throws SQLException;

	public void queryStreamTest(String requestingUser, String streamOwner, String streamName,
			String startTime, String endTime, String filter, int limit,
			int offset) throws SQLException;

	public void addOrUpdateMacro(String owner, Macro macro) throws SQLException;

	public List<Macro> getMacros(String owner) throws SQLException;

	public void deleteAllMacros(String owner) throws SQLException;

	public void deleteMacro(String owner, int id, String name) throws SQLException;
}
