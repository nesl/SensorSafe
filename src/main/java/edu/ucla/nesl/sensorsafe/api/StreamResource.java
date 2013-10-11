package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("/streams/{stream_name}")
@Produces(MediaType.TEXT_PLAIN)
@Api(value = "/streams/{stream_name}", description = "Operation about an individual stream.")
public class StreamResource {

	@Context 
	private HttpServletRequest httpReq;

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@ApiOperation(value = "Bulkload tuples to a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	@Path("/bulkload")
	public ResponseMsg doBulkLoadPost(@PathParam("stream_name") String streamName, String data) {
		
		try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
			db.bulkLoad(httpReq.getRemoteUser(), streamName, data);
		} catch (SQLException | IOException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
		
		return new ResponseMsg("Successfully completed bulkloading.");
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Add a new tuple to the stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})
	public String doPost(@PathParam("stream_name") String streamName, String strTuple) {
		try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
			db.addTuple(httpReq.getRemoteUser(), streamName, strTuple);
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		}

		return "Successfully added the tuple.";
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Retrieve the stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public String doGet(@PathParam("stream_name") String streamName, 
			@QueryParam("start_time") String startTime, 
			@QueryParam("end_time") String endTime, 
			@QueryParam("expr") String expr) {

		String ret = null;
		try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();

			Stream stream = db.getStreamInfo(httpReq.getRemoteUser(), streamName);
			JSONArray tuples = db.queryStream(httpReq.getRemoteUser(), streamName, startTime, endTime, expr);

			ObjectMapper mapper = new ObjectMapper();
			AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
			mapper.setAnnotationIntrospector(introspector);
			JSONObject json = (JSONObject)JSONValue.parse(mapper.writeValueAsString(stream));

			json.put("tuples", tuples);
			ret = json.toString();
		} catch (SQLException | JsonProcessingException | UnsupportedOperationException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		}

		return ret;
	}	

	@DELETE
	@ApiOperation(value = "Delete a stream.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public String doDelete(@PathParam("stream_name") String streamName,
			@QueryParam("start_time") String startTime, 
			@QueryParam("end_time") String endTime) {

		try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
			db.deleteStream(httpReq.getRemoteUser(), streamName, startTime, endTime);
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		}
		return "Succefully deleted stream (" + streamName + ") as requested.";
	}	
}
