package edu.ucla.nesl.sensorsafe.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.Macro;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@RolesAllowed(Roles.OWNER)
@Path("macros")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "macros", description = "Operation about macros.")
public class MacroResource {

	@Context
	private SecurityContext securityContext;

	@GET
    @ApiOperation(value = "List current macros.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<Macro> doGet() throws JsonProcessingException {
		String ownerName = securityContext.getUserPrincipal().getName();
		List<Macro> macros;
    	StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			macros = db.getMacros(ownerName);
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
    	
    	return macros;
	}

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new macro", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doPost(
    		@ApiParam(name = "macro", value = "You can use macro in rule condition or query filter by $(MACRO_NAME). "
    				+ "They will be replaced with the value.")
    		@Valid Macro macro) throws JsonProcessingException {
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		db.addOrUpdateMacro(ownerName, macro);
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
    	
    	return new ResponseMsg("Sucessfully added/updated the macro.");
    }
    
    @DELETE
    @ApiOperation(value = "Delete macros.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doDelete(
    		@QueryParam("id") int id,
    		@QueryParam("macro_name") String macroName) throws JsonProcessingException {
    	
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		if (id > 0 || macroName != null) 
    			db.deleteMacro(ownerName, id, macroName);
    		else 
    			db.deleteAllMacros(ownerName);
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
    	return new ResponseMsg("Successfully deleted macro(s).");
    }
}
