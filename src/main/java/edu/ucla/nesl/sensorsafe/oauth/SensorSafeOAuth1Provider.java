package edu.ucla.nesl.sensorsafe.oauth;

import java.io.IOException;
import java.security.Principal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.NamingException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;
import org.glassfish.jersey.server.oauth1.OAuth1Consumer;
import org.glassfish.jersey.server.oauth1.OAuth1Provider;
import org.glassfish.jersey.server.oauth1.OAuth1Token;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.auth.SensorSafePrincipal;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.User;

/** Default in-memory implementation of OAuth1Provider. Stores consumers and tokens
 * in static hash maps. Provides some additional helper methods for consumer
 * and token management (registering new consumers, retrieving a list of all
 * registered consumers per owner, listing the authorized tokens per principal,
 * revoking tokens, etc.)
 *
 * @author Martin Matula
 * @author Miroslav Fuksa (miroslav.fuksa at oracle.com)
 */
public class SensorSafeOAuth1Provider implements OAuth1Provider {

	private static final ConcurrentHashMap<String, Consumer> consumerByConsumerKey = new ConcurrentHashMap<String, Consumer>();
	private static final ConcurrentHashMap<String, Token> accessTokenByTokenString = new ConcurrentHashMap<String, Token>();
	private static final ConcurrentHashMap<String, Token> requestTokenByTokenString = new ConcurrentHashMap<String, Token>();
	private static final ConcurrentHashMap<String, String> verifierByTokenString = new ConcurrentHashMap<String, String>();

	public SensorSafeOAuth1Provider() {
		initializeFromDatabase();
	}
	
	private void initializeFromDatabase() {
		UserDatabaseDriver db = null;
		List<User> consumers = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			consumers = db.getAllConsumers();
		} catch (ClassNotFoundException | SQLException | IOException | NamingException e) {
			e.printStackTrace();
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		for (User consumer: consumers) {
			registerConsumer(consumer.owner, consumer.oauth_consumer_key, consumer.oauth_consumer_secret
					, new MultivaluedHashMap<String, String>());
			if ( consumer.oauth_access_key != null && consumer.oauth_access_secret != null) {
				Set<String> roles = new HashSet<String>();
				roles.add(Roles.CONSUMER);		
				addAccessToken(consumer.oauth_access_key, consumer.oauth_access_secret, consumer.oauth_consumer_key
						, null, new SensorSafePrincipal(consumer.username), roles, new MultivaluedHashMap<String, String>());
			}
		}
	}
	
	@Override
	public Consumer getConsumer(String consumerKey) {
		return consumerByConsumerKey.get(consumerKey);
	}

	/**
	 * Register a new consumer.
	 *
	 * @param owner Identifier of the owner that registers the consumer (user ID or similar).
	 * @param attributes Additional attributes (name-values pairs - to store additional
	 * information about the consumer, such as name, URI, description, etc.)
	 * @return Consumer object for the newly registered consumer.
	 */
	public Consumer registerConsumer(String owner, MultivaluedMap<String, String> attributes) {
		return registerConsumer(owner, newUUIDString(), newUUIDString(), attributes);
	}

	/**
	 * Register a new consumer configured with Consumer Key.
	 *
	 * @param owner Identifier of the owner that registers the consumer (user ID or similar).
	 * @param key Consumer key.
	 * @param secret Consumer key secret.
	 * @param attributes Additional attributes (name-values pairs - to store additional
	 * information about the consumer, such as name, URI, description, etc.)
	 * @return
	 */
	public Consumer registerConsumer(String owner, String key, String secret, MultivaluedMap<String, String> attributes) {
		Consumer c = new Consumer(key, secret, owner, attributes);
		consumerByConsumerKey.put(c.getKey(), c);
		return c;
	}


	/** Returns a set of consumers registered by a given owner.
	 *
	 * @param owner Identifier of the owner that registered the consumers to be retrieved.
	 * @return consumers registered by the owner.
	 */
	public Set<Consumer> getConsumers(String owner) {
		Set<Consumer> result = new HashSet<Consumer>();
		for (Consumer consumer : consumerByConsumerKey.values()) {
			if (consumer.getOwner().equals(owner)) {
				result.add(consumer);
			}
		}
		return result;
	}

	/** Returns a list of access tokens authorized with the supplied principal name.
	 *
	 * @param principalName Principal name for which to retrieve the authorized tokens.
	 * @return authorized access tokens.
	 */
	public Set<Token> getAccessTokens(String principalName) {
		Set<Token> tokens = new HashSet<Token>();
		for (Token token : accessTokenByTokenString.values()) {
			if (principalName.equals(token.getPrincipal().getName())) {
				tokens.add(token);
			}
		}
		return tokens;
	}

	/** Authorizes a request token for given principal and roles and returns
	 * verifier.
	 *
	 * @param token Request token to authorize.
	 * @param userPrincipal User principal to authorize the token for.
	 * @param roles Set of roles to authorize the token for.
	 * @return OAuth verifier value for exchanging this token for an access token.
	 */
	public String authorizeToken(Token token, Principal userPrincipal, Set<String> roles) {
		Token authorized = token.authorize(userPrincipal, roles);
		requestTokenByTokenString.put(token.getToken(), authorized);
		String verifier = newUUIDString();
		verifierByTokenString.put(token.getToken(), verifier);
		return verifier;
	}

	/** Checks if the supplied token is authorized for a given principal name
	 * and if so, revokes the authorization.
	 *
	 * @param token Access token to revoke the authorization for.
	 * @param principalName Principal name the token is currently authorized for.
	 */
	public void revokeAccessToken(String token, String principalName) {
		Token t = (Token) getAccessToken(token);
		if (t != null && t.getPrincipal().getName().equals(principalName)) {
			accessTokenByTokenString.remove(token);
		}
	}

	/** Generates a new non-guessable random string (used for token/customer
	 * strings, secrets and verifier.
	 *
	 * @return Random UUID string.
	 */
	protected String newUUIDString() {
		String tmp = UUID.randomUUID().toString();
		return tmp.replaceAll("-", "");
	}

	@Override
	public Token getRequestToken(String token) {
		return requestTokenByTokenString.get(token);
	}

	@Override
	public OAuth1Token newRequestToken(String consumerKey, String callbackUrl, Map<String, List<String>> attributes) {
		Token rt = new Token(newUUIDString(), newUUIDString(), consumerKey, callbackUrl, attributes);
		requestTokenByTokenString.put(rt.getToken(), rt);
		return rt;
	}

	@Override
	public OAuth1Token newAccessToken(OAuth1Token requestToken, String verifier) {
		if (verifier == null || !verifier.equals(verifierByTokenString.remove(requestToken.getToken()))) {
			return null;
		}
		Token token = requestToken == null ? null : requestTokenByTokenString.remove(requestToken.getToken());
		if (token == null) {
			return null;
		}
		String atKey = newUUIDString();
		String atSecret = newUUIDString();
		Token at = new Token(atKey, atSecret, token);
		accessTokenByTokenString.put(at.getToken(), at);
		
		// Store into database.
		UserDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getUserDatabase();
			db.addAccessToken(atKey, atSecret, token.getConsumer().getKey());
		} catch (ClassNotFoundException | SQLException | IOException | NamingException e) {
			e.printStackTrace();
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return at;
	}

	public void addAccessToken(String token, String secret, String consumerKey, String callbackUrl,
			Principal principal, Set<String> roles, MultivaluedMap<String, String> attributes) {
		final Token accessToken = new Token(token, secret, consumerKey, callbackUrl, principal, roles, attributes);

		accessTokenByTokenString.put(accessToken.getToken(), accessToken);
	}

	@Override
	public OAuth1Token getAccessToken(String token) {
		return accessTokenByTokenString.get(token);
	}

	/** Simple read-only implementation of {@link OAuth1Consumer}.
	 */
	public static class Consumer implements OAuth1Consumer {
		private final String key;
		private final String secret;
		private final String owner;
		private final MultivaluedMap<String, String> attributes;

		private Consumer(String key, String secret, String owner, Map<String, List<String>> attributes) {
			this.key = key;
			this.secret = secret;
			this.owner = owner;
			this.attributes = getImmutableMap(attributes);
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public String getSecret() {
			return secret;
		}

		/** Returns identifier of owner of this consumer - i.e. who registered
		 * the consumer.
		 *
		 * @return consumer owner
		 */
		public String getOwner() {
			return owner;
		}

		/** Returns additional attributes associated with the consumer (e.g. name,
		 * URI, description, etc.)
		 *
		 * @return name-values pairs of additional attributes
		 */
		public MultivaluedMap<String, String> getAttributes() {
			return attributes;
		}

		@Override
		public Principal getPrincipal() {
			return null;
		}

		@Override
		public boolean isInRole(String role) {
			return false;
		}
	}


	private static MultivaluedMap<String, String> getImmutableMap(Map<String, List<String>> map) {
		final MultivaluedHashMap<String, String> newMap = new MultivaluedHashMap<String, String>();
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			newMap.put(entry.getKey(), entry.getValue());
		}
		return newMap;
	}


	/** Simple immutable implementation of {@link OAuth1Token}.
	 *
	 */
	public class Token implements OAuth1Token {
		private final String token;
		private final String secret;
		private final String consumerKey;
		private final String callbackUrl;
		private final Principal principal;
		private final Set<String> roles;
		private final MultivaluedMap<String, String> attribs;

		protected Token(String token, String secret, String consumerKey, String callbackUrl,
				Principal principal, Set<String> roles, MultivaluedMap<String, String> attributes) {
			this.token = token;
			this.secret = secret;
			this.consumerKey = consumerKey;
			this.callbackUrl = callbackUrl;
			this.principal = principal;
			this.roles = roles;
			this.attribs = attributes;
		}

		public Token(String token, String secret, String consumerKey, String callbackUrl, Map<String, List<String>> attributes) {
			this(token, secret, consumerKey, callbackUrl, null, Collections.<String>emptySet(),
					new ImmutableMultivaluedMap<String, String>(getImmutableMap(attributes)));
		}

		public Token(String token, String secret, Token requestToken) {
			this(token, secret, requestToken.getConsumer().getKey(), null,
					requestToken.principal, requestToken.roles, ImmutableMultivaluedMap.<String, String>empty());
		}

		@Override
		public String getToken() {
			return token;
		}

		@Override
		public String getSecret() {
			return secret;
		}

		@Override
		public OAuth1Consumer getConsumer() {
			return SensorSafeOAuth1Provider.this.getConsumer(consumerKey);
		}

		@Override
		public MultivaluedMap<String, String> getAttributes() {
			return attribs;
		}

		@Override
		public Principal getPrincipal() {
			return principal;
		}

		@Override
		public boolean isInRole(String role) {
			return roles.contains(role);
		}

		/** Returns callback URL for this token (applicable just to request tokens)
		 *
		 * @return callback url
		 */
		public String getCallbackUrl() {
			return callbackUrl;
		}

		/** Authorizes this token - i.e. generates a clone with principal and roles set
		 * to the passed values.
		 *
		 * @param principal Principal to add to the token.
		 * @param roles Roles to add to the token.
		 * @return Cloned token with the principal and roles set.
		 */
		protected Token authorize(Principal principal, Set<String> roles) {
			return new Token(token, secret, consumerKey, callbackUrl, principal, roles == null ? Collections.<String>emptySet() : new HashSet<String>(roles), attribs);
		}
	}
}
