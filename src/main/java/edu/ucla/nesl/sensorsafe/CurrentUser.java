package edu.ucla.nesl.sensorsafe;

import edu.ucla.nesl.sensorsafe.model.User;

public class CurrentUser {
	
	private static boolean isLoggedin = false;
	private static boolean isOwner = false;
	
	private static User userinfo;
	
	public static boolean isOwner() {
		return isLoggedin ? isOwner : false;
	}
	
	public static String getEmail() {
		return isLoggedin ? userinfo.email : null;
	}
	
	public static String getFirstName() {
		return isLoggedin ? userinfo.firstName : null;
	}
	
	public static String getLastName() {
		return isLoggedin ? userinfo.lastName : null;
	}

	public static void login(User _userinfo) {
		isLoggedin = true;
		userinfo = _userinfo;
	}
	
	public static User getUserInfo() {
		return userinfo;
	}
	
	public static void logout() {
		isLoggedin = false;
	}
}
