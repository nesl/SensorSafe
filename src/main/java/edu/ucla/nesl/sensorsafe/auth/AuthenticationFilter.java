package edu.ucla.nesl.sensorsafe.auth;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.Priority;
import javax.naming.NamingException;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.User;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Provider // IMPORTANT: This annotation is required.
@Priority(Priorities.AUTHENTICATION-1) // This make sure use OAuth if it presents.
public class AuthenticationFilter implements ContainerRequestFilter {

	public static final String[] API_KEY_HEADERS = { "Api-Key", "Apikey", "X-Api-Key", "X-Apikey" };
	public static final String AUTHORIZATION_HEADER = "Authorization";

	@Override
	public void filter(ContainerRequestContext request) throws IOException {
		
		// Dump Headers
		/*for (Entry<String, List<String>> entry: request.getHeaders().entrySet()) {
			String key = entry.getKey();
			String values = "";
			for (String value : entry.getValue()) {
				values += value + ", ";
			}
			values = values.substring(0, values.length() - 2);
			Log.info(key + ": " + values);
		}*/
		
		String authorization = request.getHeaderString(AUTHORIZATION_HEADER);
		String apikey = null;
		for (String apikeyHeader: API_KEY_HEADERS) {
			String value = request.getHeaderString(apikeyHeader);
			if (value != null) {
				apikey = value;
			}
		}

		// Use OAuth > Apikey > HTTP BASIC 
		if (apikey != null) {
			performApiKeyAuthentication(request, apikey);
		} else 	if (authorization != null && (authorization.startsWith("Basic") || authorization.startsWith("basic"))) {
			performHttpBasicAuthentication(request, authorization);
		}
	}

	private String[] decodeBasicAuth(String auth) {
		//Replacing "Basic THE_BASE_64" to "THE_BASE_64" directly
		auth = auth.replaceFirst("[B|b]asic ", "");

		//Decode the Base64 into byte[]
		byte[] decodedBytes = DatatypeConverter.parseBase64Binary(auth);

		//If the decode fails in any case
		if(decodedBytes == null || decodedBytes.length == 0){
			return null;
		}

		//Now we can convert the byte[] into a splitted array :
		//  - the first one is login,
		//  - the second one password
		return new String(decodedBytes).split(":", 2);
	}

	private void performHttpBasicAuthentication(ContainerRequestContext request, String basicAuthHeader) throws JsonProcessingException {

		//lap : loginAndPassword
		String[] lap = decodeBasicAuth(basicAuthHeader);

		//If login or password fail
		if(lap == null || lap.length != 2){
			throw new WebApplicationException(Response.status(Status.UNAUTHORIZED).header("WWW-Authenticate", "Basic").build());
		}

		String username = lap[0];
		String password = lap[1];

		UserDatabaseDriver db = null;
		User dbUser = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			dbUser = db.getUser(username);
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

		if (dbUser == null || !password.equals(dbUser.password)) {
			throw new WebApplicationException(Response.status(Status.UNAUTHORIZED).header("WWW-Authenticate", "Basic").build());
		}

		SecurityContext sc = new SensorSafeSecurityContext(dbUser.username, dbUser.role, request.getSecurityContext().isSecure());
		request.setSecurityContext(sc);
	}

	private void performApiKeyAuthentication(ContainerRequestContext request, String apikey) {

		SecurityContext sc = request.getSecurityContext();

		User user = null;
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			user = db.getUserByApikey(apikey);
		} catch (ClassNotFoundException | SQLException | IOException | NamingException e) {
			e.printStackTrace();
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}		

		if (user != null) {
			request.setSecurityContext(new SensorSafeSecurityContext(user.username, user.role, sc.isSecure()));
		}
	}
}