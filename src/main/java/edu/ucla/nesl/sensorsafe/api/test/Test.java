package edu.ucla.nesl.sensorsafe.api.test;

import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("test")
public class Test {

    @GET
    public String doGet() throws NoSuchAlgorithmException {
    	KeyGenerator keygenerator = KeyGenerator.getInstance("AES");
        SecretKey myDesKey = keygenerator.generateKey();
        
        return myDesKey.getEncoded().toString().split("@")[1];
    }
}
