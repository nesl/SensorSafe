package edu.ucla.nesl.sensorsafe.api;

import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import edu.ucla.nesl.sensorsafe.SensorSafeSwaggerConfig;

@Path("doc")
public class Doc {
	
	@Context 
	private HttpServletRequest httpReq;
	
	@Context 
	private HttpServletResponse httpRes;

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
