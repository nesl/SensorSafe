package edu.ucla.nesl.sensorsafe.auth;

import java.security.Principal;
import javax.ws.rs.core.SecurityContext;

public class SensorSafeSecurityContext implements SecurityContext {

	private static final String SCHEME = "Api-Key";

	private final String username;
	private final String role;
	private final boolean isSecure;

	public SensorSafeSecurityContext(String username, String role, boolean isSecure) {
		this.username = username;
		this.role = role;
		this.isSecure = isSecure;
	}

	@Override
	public Principal getUserPrincipal() {
		return new SensorSafePrincipal(username);
	}

	@Override
	public boolean isUserInRole(String role) {
		return role.equals(this.role);
	}

	@Override
	public boolean isSecure() {
		return isSecure;
	}

	@Override
	public String getAuthenticationScheme() {
		return SCHEME;
	}
}
