package edu.ucla.nesl.sensorsafe;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;

import edu.ucla.nesl.sensorsafe.tools.Log;

@Provider // IMPORTANT: This annotation is required.
public class SensorSafeRequestFilter implements ContainerRequestFilter {
 
	@Override
	public void filter(ContainerRequestContext requestContext)
			throws IOException {

		// TODO Apikey authentication here.
	}
}