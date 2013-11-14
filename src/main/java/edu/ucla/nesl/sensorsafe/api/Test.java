package edu.ucla.nesl.sensorsafe.api;

import java.security.Principal;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import edu.ucla.nesl.sensorsafe.auth.Roles;

@Path("test")
public class Test {

	@Context
	private SecurityContext sc;
	
	@RolesAllowed(Roles.CONSUMER)
    @GET
    public String doGet() {
    	Principal principal = sc.getUserPrincipal();
    	return "You're " + (principal == null ? "null" : principal.getName());
    }
}
