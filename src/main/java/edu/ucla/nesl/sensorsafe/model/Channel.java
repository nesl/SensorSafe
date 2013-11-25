package edu.ucla.nesl.sensorsafe.model;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Channel definition.")
@XmlRootElement
public class Channel {
	
	@XmlElement(name = "name")
	@ApiModelProperty(value = "Channel name", required = true)
	@NotNull
	public String name;
	
	@XmlElement(name = "type")
	@ApiModelProperty(value = "Channel type", required = true, allowableValues = "float,int,text")
	@NotNull
	public String type;
	
	@XmlElement(name = "statistics")
	public Statistics statistics;

	public Channel() {}
	
	public Channel(String name, String type) {
		this.name = name;
		this.type = type;
	}
}
