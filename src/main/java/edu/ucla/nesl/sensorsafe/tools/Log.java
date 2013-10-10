package edu.ucla.nesl.sensorsafe.tools;

public class Log {
	
	public static void info(String msg) {
		System.out.println("INFO: " + msg);
	}
	
	public static void error(String msg) {
		System.err.println("ERROR: " + msg);
	}
}
