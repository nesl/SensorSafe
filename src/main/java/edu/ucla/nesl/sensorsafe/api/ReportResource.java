package edu.ucla.nesl.sensorsafe.api;

import java.io.File;
import java.io.IOException;

import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.CreateReportRunnable;
import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.Log;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("report")
@Produces(MediaType.APPLICATION_JSON)
public class ReportResource {

	@Context
	private SecurityContext securityContext;

	@Context 
	private HttpServletRequest httpReq;

	private DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	
	@RolesAllowed({ Roles.OWNER, Roles.CONSUMER })
	@GET
	@Path("/create")
	public ResponseMsg createReport(
			@QueryParam("start_time") String startTime,
			@QueryParam("end_time") String endTime) 
					throws JsonProcessingException {
		String owner = securityContext.getUserPrincipal().getName();

		Log.info("Report creation request: " + owner + ", " + startTime + ", " + endTime);
		
		if (startTime == null || endTime == null) {
			throw WebExceptionBuilder.buildBadRequest("start_time and end_time query parameter required.");
		}
		
		DateTime parsedStartDateTime, parsedEndDateTime;
		
		try {
			parsedStartDateTime = fmt.parseDateTime(startTime);
			parsedEndDateTime = fmt.parseDateTime(endTime);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest("Invalid query parameter.");
		}
		
		synchronized (CreateReportRunnable.reportLock) {
			File lockFile = new File(CreateReportRunnable.REPORT_LOCK_FILE);
			if (lockFile.exists()) {
				throw WebExceptionBuilder.buildBadRequest("Report creation job is already running..");
			}
		}
		
		String addr = httpReq.getLocalAddr();
		int port = httpReq.getLocalPort();
		boolean isHttps = httpReq.isSecure();
		
		String basePath;
		if (isHttps) {
			basePath = "https://";
		} else { 
			basePath = "http://";
		}
		basePath += addr + ":" + port;

		new Thread(new CreateReportRunnable(parsedStartDateTime, parsedEndDateTime, owner, basePath)).start();
		
		return new ResponseMsg(owner + "'s report creation job started: " + fmt.print(parsedStartDateTime) + " ~ " + fmt.print(parsedEndDateTime));
	}
	
	@POST
	@Path("/submit")
	public ResponseMsg submit(String data, @QueryParam("target") String target) throws JsonProcessingException {
		
		Log.info("target: " + target);
		Log.info("data: " + data);
		
		File destFile = new File(CreateReportRunnable.REPORT_ROOT + "/" + target + "/survey.csv");
		if (!destFile.exists()) {
			try {
				destFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				throw WebExceptionBuilder.buildInternalServerError(e);
			}
		}
		
		try {
			FileUtils.writeStringToFile(destFile, data);
		} catch (IOException e) {
			e.printStackTrace();
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
		
		return new ResponseMsg("Success!");
	}
}
