package edu.ucla.nesl.sensorsafe.init;

import javax.ws.rs.core.Feature;

import org.glassfish.jersey.server.ResourceConfig;

import edu.ucla.nesl.sensorsafe.auth.RolesAllowedDynamicFeature;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1Provider;
import edu.ucla.nesl.sensorsafe.oauth.SensorSafeOAuth1ServerFeature;

public class SensorSafeResourceConfig extends ResourceConfig {

	public static final SensorSafeOAuth1Provider oauthProvider = new SensorSafeOAuth1Provider();
	public static final String REQUEST_TOKEN_API_PATH = "/oauth/request_token";
	public static final String ACCESS_TOKEN_API_PATH = "/oauth/access_token";
	public static final String AUTHORIZE_API_PATH = "/oauth/authorize";
	
	public SensorSafeResourceConfig() {
		
		// Register OAuthServerFeature
		Feature oauthFeature = new SensorSafeOAuth1ServerFeature(oauthProvider, REQUEST_TOKEN_API_PATH, ACCESS_TOKEN_API_PATH);
		register(oauthFeature);
		
		// Register RolesAllowedDynamicFeature
		register(RolesAllowedDynamicFeature.class);
	}
}
