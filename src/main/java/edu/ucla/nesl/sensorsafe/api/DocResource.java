package edu.ucla.nesl.sensorsafe.api;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import edu.ucla.nesl.sensorsafe.init.SensorSafeSwaggerConfig;

@Path("doc")
@PermitAll
public class DocResource {
	
	@Context 
	private HttpServletRequest httpReq;
	
	@GET
	public Response doGet() throws URISyntaxException {
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
		SensorSafeSwaggerConfig.setBasePath(apiBasePath);
		
		return Response.temporaryRedirect(new URI(basePath + "/api-docs")).build();
	}
}
