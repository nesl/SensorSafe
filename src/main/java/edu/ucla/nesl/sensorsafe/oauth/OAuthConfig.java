package edu.ucla.nesl.sensorsafe.oauth;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Feature;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.oauth1.DefaultOAuth1Provider;
import org.glassfish.jersey.server.oauth1.OAuth1ServerFeature;

public class OAuthConfig extends ResourceConfig {

	public OAuthConfig() {
		DefaultOAuth1Provider provider = new DefaultOAuth1Provider();
		
		MultivaluedMap<String, String> attr = new MultivaluedHashMap<String, String>();
		List<String> values = new ArrayList<String>();
		values.add("hello");
		attr.put("desc", values);
		provider.registerConsumer("haksoo", "kFojlXuZHg4yYnFAplWJHw", "0G6ZcVQhUiZ7RBo8j9MpAFHXyqCI4nLLMou3SWtLe14", attr);
		
		Feature oauthFeature = new OAuth1ServerFeature(provider, "/request_token", "/access_token");
		
		registerInstances(oauthFeature);
	}
	
}
