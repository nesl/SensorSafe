package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.UserDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@RolesAllowed(Roles.ADMIN)
@Path("debug")
@Api(value = "debug", description = "Various operations for debugging.")
public class DebugResource {
	
    @GET
    @Path("/init_stream_db")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Initialize stream database.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg initStreamDb() {
    	return new ResponseMsg("Disabled API.");
    	
    	/*StreamDatabaseDriver streamDb = null;
    	try {
			streamDb = DatabaseConnector.getStreamDatabase();
			streamDb.clean();
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (streamDb != null) {
				try {
					streamDb.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
        return new ResponseMsg("Stream database initialized.");*/
    }

    @GET
    @Path("/init_user_db")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Initialize user database.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg initUserDb() {
    	UserDatabaseDriver userDb = null;
    	try {
			userDb = DatabaseConnector.getUserDatabase();
			userDb.clean();
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} finally {
			if (userDb != null) {
				try {
					userDb.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
        return new ResponseMsg("User database initialized.");
    }
}
