package edu.ucla.nesl.sensorsafe.api.test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("test")
public class Test {

    @GET
    public String doGet() {
    	return "hello";
    }
}
