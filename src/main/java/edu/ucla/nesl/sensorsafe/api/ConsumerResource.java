package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.mail.MessagingException;
import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.SecurityContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.init.SensorSafeResourceConfig;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.User;
import edu.ucla.nesl.sensorsafe.tools.MailSender;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("consumers")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "consumers", description = "Operations about consumers.")
public class ConsumerResource {
	
	@Context
	private SecurityContext securityContext;

	@Context 
	private HttpServletRequest httpReq;

	@RolesAllowed(Roles.OWNER)
	@GET
    @ApiOperation(value = "List currently registered consumers on this server.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<User> getConsumerList() throws JsonProcessingException {
		String ownerName = securityContext.getUserPrincipal().getName();
		UserDatabaseDriver db = null;
		List<User> users = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			users = db.getConsumers(ownerName);
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
		return users;
	}

	private String getApiBasePath() {
		String addr = httpReq.getLocalAddr();
		int port = httpReq.getLocalPort();
		boolean isHttps = httpReq.isSecure();
		
		String basePath;
		if (isHttps) {
			basePath = "https://";
		} else { 
			basePath = "http://";
		}
		basePath += addr + ":" + port;
		String apiBasePath = basePath + "/api";
		return apiBasePath;
	}
	
	@RolesAllowed(Roles.OWNER)
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add a new consumer.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg addNewConsumer(
			@ApiParam(name = "new_consumer"
						, value = "Only username and password required.  If email is provided, "
								+ "the server will send an email with "
								+ "Apikey, OAuth Consumer Key, and Secret."
						, required = true)
			User newConsumer) throws JsonProcessingException {
		String ownerName = securityContext.getUserPrincipal().getName();
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			newConsumer = db.addConsumer(newConsumer, ownerName);
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

		SensorSafeResourceConfig.oauthProvider.registerConsumer(ownerName, newConsumer.oauth_consumer_key, newConsumer.oauth_consumer_secret, new MultivaluedHashMap<String, String>());
		
		if (newConsumer.email != null) {
			try {
				MailSender.sendConsumerEmail(newConsumer, ownerName, getApiBasePath());
			} catch (MessagingException e) {
				throw WebExceptionBuilder.buildInternalServerError(e);
			}
		}
		
		return new ResponseMsg("Successfully added a consumer.");
	}
	
	@RolesAllowed(Roles.OWNER)
	@DELETE
    @ApiOperation(value = "Delete a consumer.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public ResponseMsg deleteConsumer(
			@ApiParam(name = "username", value = "Enter a username you want to delete.", required = true)
			@QueryParam("username") String consumerName) throws JsonProcessingException {
		
		String ownerName = securityContext.getUserPrincipal().getName();
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			User deletedConsumer = db.deleteConsumer(consumerName, ownerName);
			if (deletedConsumer != null && deletedConsumer.oauth_access_key != null) {
				SensorSafeResourceConfig.oauthProvider.revokeAccessToken(deletedConsumer.oauth_access_key, deletedConsumer.username);
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
					e.printStackTrace();
				}
			}
		}
		return new ResponseMsg("Successfully deleted a consumer.");
	}
}
