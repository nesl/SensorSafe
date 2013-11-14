package edu.ucla.nesl.sensorsafe.init;

import javax.ws.rs.core.Feature;

import org.glassfish.jersey.server.ResourceConfig;

import edu.ucla.nesl.sensorsafe.auth.RolesAllowedDynamicFeature;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1Provider;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1ServerFeature;

public class SensorSafeResourceConfig extends ResourceConfig {

	public static final SensorSafeOAuth1Provider oauthProvider = new SensorSafeOAuth1Provider();

	public SensorSafeResourceConfig() {
		
		// Register OAuthServerFeature
		Feature oauthFeature = new SensorSafeOAuth1ServerFeature(oauthProvider, "/oauth/request_token", "/oauth/access_token");
		register(oauthFeature);
		
		// Register RolesAllowedDynamicFeature
		register(RolesAllowedDynamicFeature.class);
	}
}
