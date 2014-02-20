package edu.ucla.nesl.sensorsafe.api;

import java.io.File;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.CreateReportRunnable;
import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("report")
@Produces(MediaType.APPLICATION_JSON)
public class ReportResource {

	@Context
	private SecurityContext securityContext;

	private DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
	
	@RolesAllowed({ Roles.OWNER, Roles.CONSUMER })
	@GET
	@Path("/create")
	public ResponseMsg createReport(@QueryParam("date") String date) throws JsonProcessingException {
		String owner = securityContext.getUserPrincipal().getName();

		if (date == null) {
			throw WebExceptionBuilder.buildBadRequest("date query parameter required.");
		}
		
		DateTime parsedDateTime;
		try {
			parsedDateTime = fmt.parseDateTime(date);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest("Invalid date query parameter.");
		}
		
		synchronized (CreateReportRunnable.reportLock) {
			File lockFile = new File(CreateReportRunnable.REPORT_LOCK_FILE);
			if (lockFile.exists()) {
				throw WebExceptionBuilder.buildBadRequest("Report creation job already running..");
			}
		}
		
		new Thread(new CreateReportRunnable(parsedDateTime, owner)).start();
		
		return new ResponseMsg(owner + "'s report creation job started: " + fmt.print(parsedDateTime));
	}
}
