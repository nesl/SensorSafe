package edu.ucla.nesl.sensorsafe.model;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Template rule paramters.")
@XmlRootElement
public class TemplateParameter {

	@XmlElement(name = "name")
	@ApiModelProperty(value = "Parameter name", required = true)
	@NotNull
	public String name;

	@XmlElement(name = "value")
	@ApiModelProperty(value = "Parameter value", required = true)
	@NotNull
	public String value;
	
	public TemplateParameter() {}
}
