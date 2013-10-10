package edu.ucla.nesl.sensorsafe.openid;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openid4java.OpenIDException;
import org.openid4java.consumer.ConsumerException;
import org.openid4java.consumer.ConsumerManager;
import org.openid4java.consumer.VerificationResult;
import org.openid4java.discovery.DiscoveryInformation;
import org.openid4java.discovery.Identifier;
import org.openid4java.message.AuthRequest;
import org.openid4java.message.AuthSuccess;
import org.openid4java.message.ParameterList;
import org.openid4java.message.ax.AxMessage;
import org.openid4java.message.ax.FetchRequest;
import org.openid4java.message.ax.FetchResponse;

import edu.ucla.nesl.sensorsafe.CurrentUser;
import edu.ucla.nesl.sensorsafe.model.User;

public class Consumer {

	private static Consumer instance;
	public ConsumerManager manager;

	public static Consumer getInstance() throws ConsumerException {
		if (instance == null) {
			instance = new Consumer();
		}
		return instance;
	}

	private Consumer() throws ConsumerException {
		manager = new ConsumerManager();
	}

	// --- placing the authentication request ---
	public String authRequest(
			String userSuppliedString, 
			String returnToUrl, 
			ServletContext context, 
			HttpServletRequest httpReq, 
			HttpServletResponse httpResp) 
					throws OpenIDException, IOException, ServletException {

		// --- Forward proxy setup (only if needed) ---
		// ProxyProperties proxyProps = new ProxyProperties();
		// proxyProps.setProxyName("proxy.example.com");
		// proxyProps.setProxyPort(8080);
		// HttpClientFactory.setProxyProperties(proxyProps);

		// perform discovery on the user-supplied identifier
		List discoveries = manager.discover(userSuppliedString);

		// attempt to associate with the OpenID provider
		// and retrieve one service endpoint for authentication
		DiscoveryInformation discovered = manager.associate(discoveries);

		// store the discovery information in the user's session
		httpReq.getSession().setAttribute("openid-disc", discovered);

		// obtain a AuthRequest message to be sent to the OpenID provider
		AuthRequest authReq = manager.authenticate(discovered, returnToUrl);

		// Attribute Exchange example: fetching the 'email' attribute
		FetchRequest fetch = FetchRequest.createFetchRequest();
		fetch.addAttribute("email", "http://schema.openid.net/contact/email", true);
		fetch.addAttribute("firstName", "http://openid.net/schema/namePerson/first", true);
		fetch.addAttribute("lastName", "http://openid.net/schema/namePerson/last", true);		
		
		// attach the extension to the authentication request
		authReq.addExtension(fetch);


		/*if (! discovered.isVersion2() )
		{*/
			// Option 1: GET HTTP-redirect to the OpenID Provider endpoint
			// The only method supported in OpenID 1.x
			// redirect-URL usually limited ~2048 bytes
			httpResp.sendRedirect(authReq.getDestinationUrl(true));
			//return null;
		/*}
		else
		{
			// Option 2: HTML FORM Redirection (Allows payloads >2048 bytes)

			RequestDispatcher dispatcher =
					context.getRequestDispatcher("formredirection.jsp");
			httpReq.setAttribute("parameterMap", authReq.getParameterMap());
			httpReq.setAttribute("destinationUrl", authReq.getDestinationUrl(false));
			dispatcher.forward(httpReq, httpResp);
		}*/

		return null;
	}

	// --- processing the authentication response ---
	public boolean verifyResponse(HttpServletRequest httpReq) throws OpenIDException
	{
		// extract the parameters from the authentication response
		// (which comes in as a HTTP request from the OpenID provider)
		ParameterList response =
				new ParameterList(httpReq.getParameterMap());

		// retrieve the previously stored discovery information
		DiscoveryInformation discovered = (DiscoveryInformation)
				httpReq.getSession().getAttribute("openid-disc");

		// extract the receiving URL from the HTTP request
		StringBuffer receivingURL = httpReq.getRequestURL();
		String queryString = httpReq.getQueryString();
		if (queryString != null && queryString.length() > 0)
			receivingURL.append("?").append(httpReq.getQueryString());

		// verify the response; ConsumerManager needs to be the same
		// (static) instance used to place the authentication request
		VerificationResult verification = manager.verify(
				receivingURL.toString(),
				response, discovered);

		// examine the verification result and extract the verified identifier
		Identifier verified = verification.getVerifiedId();
		if (verified != null)
		{
			AuthSuccess authSuccess =
					(AuthSuccess) verification.getAuthResponse();

			if (authSuccess.hasExtension(AxMessage.OPENID_NS_AX))
			{
				FetchResponse fetchResp = (FetchResponse) authSuccess
						.getExtension(AxMessage.OPENID_NS_AX);

				List emails = fetchResp.getAttributeValues("email");
				String email = (String) emails.get(0);
				List firstNames = fetchResp.getAttributeValues("firstName");
				String firstName = (String) firstNames.get(0);
				List lastNames = fetchResp.getAttributeValues("lastName");
				String lastName = (String) lastNames.get(0);
				CurrentUser.login(new User(email, firstName, lastName));
				return true;
			}
		}
		return false;
	}
}