package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
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
import javax.ws.rs.core.SecurityContext;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@RolesAllowed(Roles.OWNER)
@Path("rules")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "rules", description = "Operation about rules.")
public class RuleResource {

	@Context
	private SecurityContext securityContext;

	@GET
    @ApiOperation(value = "List current rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<Rule> doGet() {
		String ownerName = securityContext.getUserPrincipal().getName();
    	List<Rule> rules = null;
    	StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			rules = db.getRules(ownerName);
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
    @ApiOperation(value = "Add or update a rule", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doPost(
    		@ApiParam(name = "rule", value = "Please refer to the description below.")
    		@Valid Rule rule) {
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		db.addOrUpdateRule(ownerName, rule);
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
    	
    	return new ResponseMsg("Sucessfully added/updated the rule.");
    }
    
    @DELETE
    @ApiOperation(value = "Delete rules.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doDelete(@QueryParam("id") int id) {
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		if (id > 0) 
    			db.deleteRule(ownerName, id);
    		else 
    			db.deleteAllRules(ownerName);
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
