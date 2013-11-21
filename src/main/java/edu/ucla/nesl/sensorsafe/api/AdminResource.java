package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("admins")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "admins", description = "Operations about admins.")
public class AdminResource {

	/*@RolesAllowed(Roles.ADMIN)
	@GET
    @ApiOperation(value = "List currently registered admins on this server.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<User> getAdminList() {
		UserDatabaseDriver db = null;
		List<User> admins = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			admins = db.getAdmins();
		} catch (ClassNotFoundException | IOException | NamingException | SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) { 
				try {
					db.close();
				} catch (SQLException e) {
					throw WebExceptionBuilder.buildInternalServerError(e);
				}
			}
		}
		return admins;
	}*/

	@RolesAllowed(Roles.ADMIN)
	@POST
	@Path("/password")
	@Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Change admin password.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg changeAdminPassword(
			@ApiParam(name = "password", value = "Enter your new password here.", required = true) 
			String password) throws JsonProcessingException {
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
		return new ResponseMsg("Successfully changed the admin password.");
	}
}
