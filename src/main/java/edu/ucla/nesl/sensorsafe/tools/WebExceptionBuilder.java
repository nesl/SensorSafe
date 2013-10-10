package edu.ucla.nesl.sensorsafe.tools;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class WebExceptionBuilder {

	private static final String MSG_INTERNAL_SERVER_ERROR_HEADER = "500 INTERNAL SERVER ERROR";
	private static final String MSG_DATABASE_NOT_READY = "Database is not initialized.";
	
	public static WebApplicationException buildInternalServerError(Exception e) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		String stackTrace = errors.toString();
		String finalMsg = MSG_INTERNAL_SERVER_ERROR_HEADER + "\n\n" + stackTrace;		
		return new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.TEXT_PLAIN).entity(finalMsg).build());
	}

	public static WebApplicationException buildInternalServerError(String msg) {
		return new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.TEXT_PLAIN).entity(msg).build());
	}

	public static WebApplicationException buildInternalServerErrorDatabaseNotReady() {
		return new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.TEXT_PLAIN).entity(MSG_DATABASE_NOT_READY).build());
	}

	public static WebApplicationException buildNotImplemented() {
		return new WebApplicationException(Response.status(Response.Status.NOT_IMPLEMENTED).build());
	}
	
	public static WebApplicationException buildBadRequest() {
		return new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
	}

	public static WebApplicationException buildBadRequest(String msg) {
		return new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).type(MediaType.TEXT_PLAIN).entity(msg).build());
	}
}
