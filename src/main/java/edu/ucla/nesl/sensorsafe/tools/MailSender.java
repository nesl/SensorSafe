package edu.ucla.nesl.sensorsafe.tools;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import edu.ucla.nesl.sensorsafe.init.SensorSafeResourceConfig;
import edu.ucla.nesl.sensorsafe.model.User;

public class MailSender {

	private static final String USERNAME = "sensorsafe.mailer@gmail.com";
	private static final String PASSWORD = "sensorsaferocks!";
	private static Session session;

	static {
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		session = Session.getInstance(props,
				new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(USERNAME, PASSWORD);
			}
		});
	}

	public static void send(String to, String subject, String content) throws AddressException, MessagingException {
		Message message = new MimeMessage(session);
		//message.setFrom(new InternetAddress(from));
		message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
		message.setSubject(subject);
		message.setContent(content, "text/html");
		Transport.send(message);
	}

	public static void sendConsumerEmail(User newConsumer, String owner, String apiBasePath) throws AddressException, MessagingException {
		String requestTokenApi = apiBasePath + SensorSafeResourceConfig.REQUEST_TOKEN_API_PATH;
		String accessTokenApi = apiBasePath + SensorSafeResourceConfig.ACCESS_TOKEN_API_PATH;
		String authorizeApi = apiBasePath + SensorSafeResourceConfig.AUTHORIZE_API_PATH;
		
		String content = owner + " has granted you access to his/her data!<br/>"
				+ "<br/>"
				+ "Your Api-Key: " + newConsumer.apikey + "<br/>"
				+ "<br/>"
				+ "OAuth1 Access Information:<br/>"
				+ "Consumer Key: " + newConsumer.oauthConsumerKey + "<br/>"
				+ "Consumer Secret: " + newConsumer.oauthConsumerSecret + "<br/>"
				+ "Request Token API: " + requestTokenApi + "<br/>"
				+ "Authroize API: " + authorizeApi + "<br/>"
				+ "Access Token API: " + accessTokenApi;
		
		send(newConsumer.email, "You can now access " + owner + "'s data!", content);
	}
}
