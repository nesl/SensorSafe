package edu.ucla.nesl.sensorsafe.api;

import java.sql.SQLException;
import java.util.List;

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

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Stream;
import edu.ucla.nesl.sensorsafe.model.StreamCollection;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("streams")
@Produces(MediaType.TEXT_PLAIN)
@Api(value = "streams", description = "Operation about stream collection.")
public class StreamCollectionResource {

	@Context 
	private HttpServletRequest httpReq;

   @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get list of streams", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public StreamCollection doGet() {    	
    	StreamCollection streamCollection;
    	try {
    		StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase(httpReq.getRemoteUser());
			List<Stream> streams = db.getStreamList();
			streamCollection = new StreamCollection(streams);
		} catch (SQLException | ClassNotFoundException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
    	return streamCollection;
    }
    
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new stream", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String doPost(@Valid Stream stream) {
    	try {
    		StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase(httpReq.getRemoteUser());
			db.createStream(stream);
		} catch (SQLException | ClassNotFoundException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		}
    	
    	return "Successfully created a new stream: " + stream.name;
    }
    
    @DELETE
    @ApiOperation(value = "Delete entire streams.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String doDelete() {
    	try {
    		StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase(httpReq.getRemoteUser());
			db.deleteAllStreams();
		} catch (SQLException | ClassNotFoundException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
    	return "Successfully deleted all streams.";
    }
}
