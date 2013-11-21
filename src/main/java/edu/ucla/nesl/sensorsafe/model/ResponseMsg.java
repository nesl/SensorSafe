package edu.ucla.nesl.sensorsafe.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlRootElement(name = "responseMsg")
public class ResponseMsg {

	@XmlElement(name = "message")
	@ApiModelProperty(value = "Response message.")
	String message;
	
	public ResponseMsg() {}
	
	public ResponseMsg(String msg) {
		this.message = msg;
	}
	
	public String encodeJson() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		AnnotationIntrospector introspector = new JaxbAnnotationIntrospector();
		mapper.setAnnotationIntrospector(introspector);
		JSONObject json = (JSONObject)JSONValue.parse(mapper.writeValueAsString(this));
		return json.toString();
	}
}
