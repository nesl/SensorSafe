package edu.ucla.nesl.sensorsafe.oauth;

import javax.ws.rs.core.Feature;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.oauth1.DefaultOAuth1Provider;
import org.glassfish.jersey.server.oauth1.OAuth1ServerFeature;

public class OAuthConfig extends ResourceConfig {

	public OAuthConfig() {
		Feature oauthFeature = new OAuth1ServerFeature(new DefaultOAuth1Provider(), "/request_token", "/access_token");
		registerInstances(oauthFeature);
	}
	
}
