package edu.ucla.nesl.sensorsafe.api.deprecated;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

import org.openid4java.OpenIDException;

import edu.ucla.nesl.sensorsafe.openid.Consumer;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("openidlogin")
public class OpenIDLoginResource {
	@Context 
	private ServletContext context;

	@GET
	public String doGet(@Context HttpServletRequest httpReq, @Context HttpServletResponse httpResp) {
		
		InputStream is = context.getResourceAsStream("/WEB-INF/web.xml");
		Log.info(is.toString());
		
		String openIDURI = "https://www.google.com/accounts/o8/id";
		String returnToUrl = "https://128.97.93.251:8443/api/openid";
		try {
			Consumer.getInstance().authRequest(openIDURI, returnToUrl, context, httpReq, httpResp);
		} catch (OpenIDException | IOException
				| ServletException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
		return null;
	}
}
