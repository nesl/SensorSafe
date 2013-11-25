package edu.ucla.nesl.sensorsafe.model;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Macro")
@XmlRootElement
public class Macro {

	@XmlElement(name = "id")
	@ApiModelProperty(value = "Unique id for this rule.")
	public int id;

	@XmlElement(name = "name")
	@ApiModelProperty(value = "Unique name for this macro.", required = true)
	@NotNull
	public String name;
	
	@XmlElement(name = "value")
	@ApiModelProperty(value = "Value of this macro.", required = true)
	@NotNull
	public String value;

	public Macro() {}
	
	public Macro(String name, String value, int id) {
		this.name = name;
		this.value = value;
		this.id = id;
	}
}
