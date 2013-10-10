package edu.ucla.nesl.sensorsafe.api.deprecated;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.openid4java.OpenIDException;

import edu.ucla.nesl.sensorsafe.openid.Consumer;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("openid")
public class OpenIDResource {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String doGet(@Context HttpServletRequest httpReq) {
		try {
			boolean isSuccess = Consumer.getInstance().verifyResponse(httpReq);
			if (!isSuccess)
				return "OpenID login failed.";
		} catch (OpenIDException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);		
		}			
		return "Succefully logged in with OpenID.";
	}
}
