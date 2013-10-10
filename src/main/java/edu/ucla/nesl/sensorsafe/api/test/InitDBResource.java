package edu.ucla.nesl.sensorsafe.api.test;

import java.sql.SQLException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import edu.ucla.nesl.sensorsafe.SensorSafeServletContext;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("initdb")
//@Api(value = "initdb", description = "Initialize database.")
public class InitDBResource {
	
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    //@ApiOperation(value = "Initialize database.", notes = "TBD")
    /*@ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })*/
    public String doGet() {
    	try {
			StreamDatabaseDriver db = SensorSafeServletContext.getStreamDatabase("haksoo");
			//db.initializeStreamTables();
		} catch (ClassNotFoundException | SQLException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		}
        return "Database initialized.";
    }
}
