package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;
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

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.RuleCollection;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("rules")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "rules", description = "Operation about rules.")
public class RuleResource {

	@Context 
	private HttpServletRequest httpReq;

	@GET
    @ApiOperation(value = "List current rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public RuleCollection doGet() {
    	RuleCollection rules;
    	StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			rules = db.getRules(httpReq.getRemoteUser());
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
    	
    	return rules;
	}

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new rule", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doPost(@Valid Rule rule) {
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		db.storeRule(httpReq.getRemoteUser(), rule);
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
    	
    	return new ResponseMsg("Sucessfully stored the rule.");
    }
    
    @DELETE
    @ApiOperation(value = "Delete rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doDelete(@QueryParam("id") int id) {
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		if (id > 0) 
    			db.deleteRule(httpReq.getRemoteUser(), id);
    		else 
    			db.deleteAllRules(httpReq.getRemoteUser());
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
    	return new ResponseMsg("Successfully deleted rule(s).");
    }
}
