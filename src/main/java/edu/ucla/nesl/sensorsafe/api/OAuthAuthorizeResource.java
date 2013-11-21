package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.auth.SensorSafePrincipal;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.init.SensorSafeResourceConfig;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1Provider;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1Provider.Token;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path(SensorSafeResourceConfig.AUTHORIZE_API_PATH)
@Api(value = SensorSafeResourceConfig.AUTHORIZE_API_PATH, description = "OAuth operations.")
public class OAuthAuthorizeResource {
	
	private static final SensorSafeOAuth1Provider oauthProvider = SensorSafeResourceConfig.oauthProvider;
	
	@Context
	private SecurityContext securityContext;
	
	@GET
	@Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Authorize OAuth Request Token.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public String authorize(
			@ApiParam(name = "oauth_token", required = true)
			@QueryParam("oauth_token") String oauthToken) throws JsonProcessingException {
		
		if (oauthToken == null) {
			throw WebExceptionBuilder.buildBadRequest("oauth_token is null.");
		}
		
		Token token = oauthProvider.getRequestToken(oauthToken);
		if (token == null) {
			throw WebExceptionBuilder.buildBadRequest("Invalid oauth_token.");
		}
		
		String consumerKey = token.getConsumer().getKey();
		
		UserDatabaseDriver db = null;
		String consumerName = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			consumerName = db.getConsumerNameByOAuthKey(consumerKey);
		} catch (ClassNotFoundException | SQLException | IOException | NamingException e) {
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
		
		if (consumerName == null) {
			throw WebExceptionBuilder.buildInternalServerError(new IllegalStateException("consumerName is null."));
		}
		
		Set<String> roles = new HashSet<String>();
		roles.add(Roles.CONSUMER);		
		String verifier = oauthProvider.authorizeToken(token, new SensorSafePrincipal(consumerName), roles);
		
		return verifier;
	}
}
