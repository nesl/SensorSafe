package edu.ucla.nesl.sensorsafe.api.test;

import java.sql.SQLException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("clean_db")
@Api(value = "clean_db", description = "Clean up database.")
public class InitDBResource {
	
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Clean up database.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String doGet() {
    	try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase();
			db.clean();
		} catch (SQLException | ClassNotFoundException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
        return "Database initialized.";
    }
}
