package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
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
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.User;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("owners")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "owners", description = "Operations about owners.")
public class OwnerResource {
	
	@Context
	private SecurityContext securityContext;
	
	@RolesAllowed({ Roles.ADMIN, Roles.OWNER })
	@GET
    @ApiOperation(value = "List currently registered owners on this server.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<User> getOwnerList() {		
		UserDatabaseDriver db = null;
		List<User> owners = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			if (securityContext.isUserInRole(Roles.ADMIN)) {
				owners = db.getOwners();
			} else if (securityContext.isUserInRole(Roles.OWNER)) {
				String ownerName = securityContext.getUserPrincipal().getName();
				User owner = db.getAnOwner(ownerName);
				owners = new ArrayList<User>();
				owners.add(owner);
			}
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
		return owners;
	}

	@RolesAllowed(Roles.ADMIN)
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add a new owner.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg addNewOwner(
			@ApiParam(name = "new_owner"
					, value = "Only username and password required.  If email is provided, "
							+ "the server will send an email with Apikey."
					, required = true)
			User newOwner) {
		
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			db.addOwner(newOwner);
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
		return new ResponseMsg("Successfully added an owner.");
	}

	@RolesAllowed(Roles.ADMIN)
	@DELETE
    @ApiOperation(value = "Delete an owner.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg deleteOwner(
			@ApiParam(name = "username", value = "Enter a username you want to delete.", required = true)
			@QueryParam("username") String ownerName) {
		Log.info(ownerName);
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			db.deleteOwner(ownerName);
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
		return new ResponseMsg("Successfully deleted an owner.");
	}

}
