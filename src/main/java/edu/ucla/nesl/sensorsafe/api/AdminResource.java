package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.User;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("admin")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "admin", description = "Admin operations.")
public class AdminResource {

	@Context 
	private HttpServletRequest httpReq;

	@GET
    @ApiOperation(value = "Log into admin account.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doGet() {		
		return new ResponseMsg("Successfully logged in as admin.");
	}

	@POST
	@Path("/owners")
	@Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register a new owner.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doPostOwners(User newOwner) {
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			db.registerOwner(newOwner);
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
		return new ResponseMsg("Successfully added new owner.");
	}
	
	@POST
	@Path("/password")
	@Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Change admin password.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doPasswordPost(
			@ApiParam(name = "password", value = "Enter your new password here.", required = true) 
			String password) {
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			db.changeAdminPassword(password);
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
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
		return new ResponseMsg("Successfully changed admin password.");
	}
}
