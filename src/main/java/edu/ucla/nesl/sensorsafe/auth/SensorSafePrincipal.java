package edu.ucla.nesl.sensorsafe.auth;

import java.security.Principal;

public class SensorSafePrincipal implements Principal {

	private final String username;
	
	public SensorSafePrincipal(String username) {
		this.username = username;
	}
	
	@Override
	public String getName() {
		return username;
	}

	@Override
	public boolean equals(Object another) {
		if (another instanceof Principal) {
			return ((Principal) another).getName().equals(username);
		} else {
			return super.equals(another);
		}
	}
	
	@Override
	public String toString() {
		return username;
	}
	
	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
