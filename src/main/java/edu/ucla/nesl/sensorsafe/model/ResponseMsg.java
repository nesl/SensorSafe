package edu.ucla.nesl.sensorsafe.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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
}
