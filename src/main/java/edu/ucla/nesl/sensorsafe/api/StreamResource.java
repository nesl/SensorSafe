package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("/streams/{stream_name}")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/streams/{stream_name}", description = "Operation about an individual stream.")
public class StreamResource {

	private static final int ROW_LIMIT_WITHOUT_HTTP_STREAMING = 100;

	@Context 
	private HttpServletRequest httpReq;

	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@ApiOperation(value = "Bulkload tuples to a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	@Path("/bulkload")
	public ResponseMsg doBulkLoadPost(@PathParam("stream_name") String streamName,
			@ApiParam(name = "str_tuple", 
			value = "<pre>Usage:\n"
					+ "timestamp 1st_channel 2nd_channel 3rd_channel ..\n"
					+ "timestamp 1st_channel 2nd_channel 3rd_channel ..\n"
					+ ".\n"
					+ ".\n"
					+ "\n"
					+ "e.g.,\n"
					+ "2013-01-01 09:20:12.12345, 12.4, 1.2, 5.5 &lt;newline&gt;\n"
					+ "2013-01-01 09:20:13.12345, 11.4, 3.2, 1.5 &lt;newline&gt;\n"
					+ "2013-01-01 09:20:14.12345, 10.4, 4.2, 7.5 &lt;newline&gt;\n"
					+ "</pre>")
	String data) {

		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.bulkLoad(httpReq.getRemoteUser(), streamName, data);
		} catch (SQLException | IOException | ClassNotFoundException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully completed bulkloading.");
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Add a new tuple to the stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	public ResponseMsg doPost(
			@PathParam("stream_name") String streamName, 
			@ApiParam(name = "str_tuple", 
					value = "<pre>Usage:\n"
					+ "[ timestamp, 1st_channel, 2nd_channel, 3rd_channel, .. ]\n"
					+ "\n"
					+ "  e.g., [ \"2013-01-01 09:20:12.12345\", 12.4, 1.2, 5.5 ]\n"
					+ "  e.g., [ null, 12.4, 1.2, 5.5 ]\n"
					+ "\n"
					+ "Or,\n"
					+ "{ \"timestamp\": timestamp\n"
					+ "  \"tuple\": [ 1st_channel, 2nd_channel, 3rd_channel, .. ] }\n"
					+ "\n"
					+ "  e.g., { \"timestamp\": \"2013-01-01 09:20:12.12345\"\n"
					+ "          \"tuple\": [ 12.4, 1.2, 5.5 ] }\n"
					+ "  e.g., { \"timestamp\": null\n"
					+ "          \"tuple\": [ 12.4, 1.2, 5.5 ] }\n"
					+ "\n"
					+ "If timestamp is null, current server time will be used.</pre>") 
			String strTuple) {

		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.addTuple(httpReq.getRemoteUser(), streamName, strTuple);
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully added the tuple.");
	}

	@GET
	@ApiOperation(value = "Retrieve the stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public Object doGet(
			@PathParam("stream_name") 					final String streamName,
			@QueryParam("http_streaming") 				final boolean isHttpStreaming,
			@QueryParam("start_time") 					final String startTime, 
			@QueryParam("end_time") 					final String endTime, 
			@QueryParam("filter") 						final String filter,
			@ApiParam(name = "function", value = "WIP. Query test function.")  
			@QueryParam("function") 					final String function ,
			@ApiParam(value = "Default value 100.") 
			@DefaultValue("100") @QueryParam("limit") 	final int limit,
			@QueryParam("offset") 						final int offset) {

		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			if (function != null) {
				if (function.equals("test")) {
					db.queryStreamTest(httpReq.getRemoteUser(), streamName, startTime, endTime, filter, limit, offset);
					return new ResponseMsg("Test function executed.");
				} else {
					throw WebExceptionBuilder.buildBadRequest("Unsupported function: " + function);
				}
			} else {
				if (!isHttpStreaming && limit > ROW_LIMIT_WITHOUT_HTTP_STREAMING) {
					throw WebExceptionBuilder.buildBadRequest("Too mcuh data requested without HTTP streaming.");
				}
				Stream stream = db.getStreamInfo(httpReq.getRemoteUser(), streamName);
				ObjectMapper mapper = new ObjectMapper();
				AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
				mapper.setAnnotationIntrospector(introspector);
				JSONObject json = (JSONObject)JSONValue.parse(mapper.writeValueAsString(stream));
				json.put("tuples", null);
				String strJson = json.toString();
				strJson = strJson.substring(0, strJson.length() - 5) + "[";

				if (!isHttpStreaming) {
					db.prepareQueryStream(httpReq.getRemoteUser(), streamName, startTime, endTime, filter, limit, offset);
					JSONArray tuple = db.getNextJsonTuple();
					if (tuple != null) {
						strJson += tuple.toString();
						while((tuple = db.getNextJsonTuple()) != null) {
							strJson += "," + tuple.toString();
						}
					}
					return strJson + "]}";
				} else {
					final String strJsonOutput = strJson;
					return new StreamingOutput() {
						@Override
						public void write(OutputStream output) throws IOException, WebApplicationException {
							StreamDatabaseDriver db = null;
							try {
								db = DatabaseConnector.getStreamDatabase();
								db.prepareQueryStream(httpReq.getRemoteUser(), streamName, startTime, endTime, filter, limit, offset);
								IOUtils.write(strJsonOutput, output);
								JSONArray tuple;
								tuple = db.getNextJsonTuple();
								if (tuple != null) {
									IOUtils.write(tuple.toString(), output);
									while((tuple = db.getNextJsonTuple()) != null) {
										IOUtils.write("," + tuple.toString(), output);
									}
								}
								IOUtils.write("]}", output);
							} catch (SQLException | ClassNotFoundException | NamingException e) {
								throw WebExceptionBuilder.buildInternalServerError(e);
							} finally {
								if (db != null) {
									try {
										db.close();
									} catch (SQLException e) {
										e.printStackTrace();
									}
								}
							}
						}
					};
				}
			}
		} catch (ClassNotFoundException | IOException | NamingException | SQLException | UnsupportedOperationException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}	


	@DELETE
	@ApiOperation(value = "Delete a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doDelete(
			@PathParam("stream_name") 	String streamName,
			@QueryParam("start_time") 	String startTime, 
			@QueryParam("end_time") 	String endTime) {

		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.deleteStream(httpReq.getRemoteUser(), streamName, startTime, endTime);
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Succefully deleted stream (" + streamName + ") as requested.");
	}	
}
