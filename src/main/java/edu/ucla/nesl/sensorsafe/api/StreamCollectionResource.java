package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.model.StreamCollection;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("streams")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "streams", description = "Operation about stream collection.")
public class StreamCollectionResource {

	@Context 
	private HttpServletRequest httpReq;

	@GET
	@ApiOperation(value = "Get list of streams", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public StreamCollection doGet() {    	
		StreamCollection streamCollection;
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			List<Stream> streams = db.getStreamList(httpReq.getRemoteUser());
			streamCollection = new StreamCollection(streams);
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
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
		return streamCollection;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Create a new stream", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doPost(@Valid Stream stream) {
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.createStream(httpReq.getRemoteUser(), stream);
		} catch (IOException | NamingException | SQLException | ClassNotFoundException e) {
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
		return new ResponseMsg("Successfully created a new stream: " + stream.name);
	}

	@DELETE
	@ApiOperation(value = "Delete entire streams.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Internal Server Error")
	})
	public ResponseMsg doDelete() {
		StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			db.deleteAllStreams(httpReq.getRemoteUser());
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
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
		return new ResponseMsg("Successfully deleted all streams.");
	}
}
