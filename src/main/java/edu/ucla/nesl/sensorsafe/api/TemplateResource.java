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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import edu.ucla.nesl.sensorsafe.auth.Roles;
import edu.ucla.nesl.sensorsafe.db.DatabaseConnector;
import edu.ucla.nesl.sensorsafe.db.StreamDatabaseDriver;
import edu.ucla.nesl.sensorsafe.model.ResponseMsg;
import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.TemplateParameterDefinition;
import edu.ucla.nesl.sensorsafe.tools.WebExceptionBuilder;

@RolesAllowed(Roles.OWNER)
@Path("templates")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "templates", description = "Operation about rule templates.")
public class TemplateResource {

	@Context
	private SecurityContext securityContext;

	@GET
    @ApiOperation(value = "List current templates.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
	public List<Rule> doGet() {
		String ownerName = securityContext.getUserPrincipal().getName();
		List<Rule> rules;
    	StreamDatabaseDriver db = null;
		try {
			db = DatabaseConnector.getStreamDatabase();
			rules = db.getTemplates(ownerName);
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
    	
    	return rules;
	}

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new template.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doPost(
    		@ApiParam(name = "rule", value = "condition field can contain parameter variables, e.g., $(PARAM_VAR), "
    				+ "and will be replaced by template definition with /api/template/create_rule API.")
    		@Valid Rule rule) {
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		db.addUpdateRuleTemplate(ownerName, rule);
		} catch (SQLException | ClassNotFoundException | IOException | NamingException e) {
			throw WebExceptionBuilder.buildInternalServerError(e);
		} catch (IllegalArgumentException e) {
			throw WebExceptionBuilder.buildBadRequest(e);
		} finally {
			if (db != null) {
				try {
					db.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
    	
    	return new ResponseMsg("Sucessfully added/updated the template.");
    }
    
    @DELETE
    @ApiOperation(value = "Delete templates.", notes = "TBD")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg doDelete(
    		@QueryParam("id") int id,
    		@QueryParam("template_name") String templateName) {
    	
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		if (id > 0 || templateName != null) 
    			db.deleteTemplate(ownerName, id, templateName);
    		else 
    			db.deleteAllTemplates(ownerName);
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
    	return new ResponseMsg("Successfully deleted template(s).");
    }
    
    @POST
    @Path("/create_rule")
    @ApiOperation(value = "Create a rule based on a template.", notes = "TBD.")
    @ApiResponses(value = {
    		@ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ResponseMsg createTemplateRule(@Valid TemplateParameterDefinition params) {
    	String ownerName = securityContext.getUserPrincipal().getName();
    	StreamDatabaseDriver db = null;
    	try {
    		db = DatabaseConnector.getStreamDatabase();
    		db.createRuleFromTemplate(ownerName, params);
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
    	return new ResponseMsg("Successfully created a rule from the template.");
    }
}
