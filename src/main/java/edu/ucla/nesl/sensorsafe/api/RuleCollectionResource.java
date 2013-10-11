package edu.ucla.nesl.sensorsafe.api;

import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.RuleCollection;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("rules")
@Produces(MediaType.TEXT_PLAIN)
@Api(value = "rules", description = "Operation about rules.")
public class RuleCollectionResource {

	@Context 
	private HttpServletRequest httpReq;

	@GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "List current rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public RuleCollection doGet() {
    	RuleCollection rules;
		try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
			rules = db.getRules(httpReq.getRemoteUser());
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
    	
    	return rules;
	}

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new rule", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String doPost(@Valid Rule rule) {
    	try {
    		StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
    		db.storeRule(httpReq.getRemoteUser(), rule);
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
    	
    	return "Sucessfully stored the rule.";
    }
    
    @DELETE
    @ApiOperation(value = "Delete entire rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String doDelete(@QueryParam("id") int id) {
    	try {
    		StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
    		if (id != 0) 
    			db.deleteRule(httpReq.getRemoteUser(), id);
    		else 
    			db.deleteAllRules(httpReq.getRemoteUser());
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
    	return "Successfully deleted rule(s).";
    }
}
