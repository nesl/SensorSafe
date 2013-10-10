package edu.ucla.nesl.sensorsafe.api.test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.CurrentUser;
import edu.ucla.nesl.sensorsafe.model.User;

@Path("userinfo")
@Produces(MediaType.TEXT_PLAIN)
//@Api(value = "userinfo", description = "Operation about a current user.")
public class UserInfoResource {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	/*@ApiOperation(value = "Get current user information.", notes = "TBD")
	@ApiResponses(value = {
			@ApiResponse(code = 500, message = "Interval Server Error")
	})*/
	public User doGet() {
		return CurrentUser.getUserInfo();
	}
}
