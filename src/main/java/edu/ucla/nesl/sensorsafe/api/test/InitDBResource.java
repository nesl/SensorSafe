package edu.ucla.nesl.sensorsafe.api.test;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@Path("clean_db")
@Api(value = "clean_db", description = "Clean up database.")
public class InitDBResource {
	
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Clean up database.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doGet() {
    	StreamDatabaseDriver db = null;
    	try {
			db = DatabaseConnector.getStreamDatabase();
			db.clean();
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
        return new ResponseMsg("Database initialized.");
    }
}
