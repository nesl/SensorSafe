package edu.ucla.nesl.sensorsafe.api;

import java.sql.SQLException;

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

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.User;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("admin")
@Api(value = "admin", description = "Admin operations.")
public class AdminResource {

	@Context 
	private HttpServletRequest httpReq;

	@GET
	@Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Log into admin account.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doGet() {		
		return new ResponseMsg("Successfully logged in as admin.");
	}

	@POST
	@Path("/owners")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Register a new owner.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doPostOwners(User newOwner) {

		try {
			UserDatabaseDriver db = SensorSafeServletContext.getUserDatabase();
			db.registerOwner(newOwner);
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
		
		return new ResponseMsg("Successfully added new owner.");
	}
	
	@POST
	@Path("/password")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Change admin password.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg doPasswordPost(
			@ApiParam(name = "password", value = "Enter your new password here.", required = true) 
			String password) {
		try {
			UserDatabaseDriver db = SensorSafeServletContext.getUserDatabase();
			db.changeAdminPassword(password);
		} catch (SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}		
		return new ResponseMsg("Successfully changed admin password.");
	}
}
