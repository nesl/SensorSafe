package edu.ucla.nesl.sensorsafe.api;

import javax.annotation.security.PermitAll;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

@Path("logout")
@Api(value = "logout", description = "Log out from current account.")
@PermitAll
public class LogOutResource {
	
	@GET
    @ApiOperation(value = "Log out from current account.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 401, message = "Unauthorized")
    })
	public String doGet() {
		throw new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
	}
}
