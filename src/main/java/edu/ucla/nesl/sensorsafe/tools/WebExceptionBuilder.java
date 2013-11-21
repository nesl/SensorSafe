package edu.ucla.nesl.sensorsafe.tools;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.model.ResponseMsg;

public class WebExceptionBuilder {

	public static WebApplicationException buildInternalServerError(Exception e) throws JsonProcessingException {
		e.printStackTrace();
		
		return new WebApplicationException(
				Response
					.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MediaType.APPLICATION_JSON)
					.entity(new ResponseMsg(e.getMessage()).encodeJson())
					.build()
				);
	}

	public static WebApplicationException buildNotImplemented() throws JsonProcessingException {
		return new WebApplicationException(
				Response
					.status(Response.Status.NOT_IMPLEMENTED)
					.type(MediaType.APPLICATION_JSON)
					.entity(new ResponseMsg(Response.Status.NOT_IMPLEMENTED.getReasonPhrase()).encodeJson())
					.build()
				);
	}

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
