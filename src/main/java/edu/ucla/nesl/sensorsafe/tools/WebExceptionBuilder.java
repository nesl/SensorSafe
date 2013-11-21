package edu.ucla.nesl.sensorsafe.tools;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.model.ResponseMsg;

public class WebExceptionBuilder {

	//private static final String MSG_INTERNAL_SERVER_ERROR_HEADER = "500 INTERNAL SERVER ERROR";
	//private static final String MSG_DATABASE_NOT_READY = "Database is not initialized.";
	
	public static WebApplicationException buildInternalServerError(Exception e) throws JsonProcessingException {
		e.printStackTrace();
		
		/*StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		String stackTrace = errors.toString();
		String finalMsg = MSG_INTERNAL_SERVER_ERROR_HEADER + "\n\n" + stackTrace;*/
		
		return new WebApplicationException(
				Response
					.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MediaType.APPLICATION_JSON)
					.entity(new ResponseMsg(e.getMessage()).encodeJson())
					.build()
				);
	}

	/*public static WebApplicationException buildInternalServerError(String msg) {
		return new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.TEXT_PLAIN).entity(msg).build());
	}*/

	/*public static WebApplicationException buildInternalServerErrorDatabaseNotReady() {
		return new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR).type(MediaType.TEXT_PLAIN).entity(MSG_DATABASE_NOT_READY).build());
	}*/

	public static WebApplicationException buildNotImplemented() throws JsonProcessingException {
		return new WebApplicationException(
				Response
					.status(Response.Status.NOT_IMPLEMENTED)
					.type(MediaType.APPLICATION_JSON)
					.entity(new ResponseMsg(Response.Status.NOT_IMPLEMENTED.getReasonPhrase()).encodeJson())
					.build()
				);
	}
	
	//public static WebApplicationException buildBadRequest() {
		//return new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).build());
	//}

	public static WebApplicationException buildBadRequest(String msg) throws JsonProcessingException {
		return new WebApplicationException(
				Response
					.status(Response.Status.BAD_REQUEST)
					.type(MediaType.APPLICATION_JSON)
					.entity(new ResponseMsg(msg).encodeJson())
					.build()
				);
	}

	public static WebApplicationException buildBadRequest(Exception e) throws JsonProcessingException {
		return buildBadRequest(e.getMessage());
	}
}
