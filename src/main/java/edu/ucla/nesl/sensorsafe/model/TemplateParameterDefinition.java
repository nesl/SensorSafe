package edu.ucla.nesl.sensorsafe.model;

import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Template rule paramters.")
@XmlRootElement
public class TemplateParameterDefinition {
	
	@XmlElement(name = "template_name")
	@ApiModelProperty(value = "Base template name", required = true)
	@NotNull
	public String template_name;
	
	@XmlElement(name = "priority")
	@ApiModelProperty(value = "Rule priority", required = true)
	@NotNull
	public int priority;
	
	@XmlElement(name = "target_users")
	@ApiModelProperty(value = "List of user names for this rule to be applied. If null, the template's target users will be used.  When both are null, applied to all users.")
	public List<String> target_users;
	
	@XmlElement(name = "target_streams")
	@ApiModelProperty(value = "List of stream names for this rule to be applied. If null, the template's target streams will be used.  When both are null, applied to all users.")
	public List<String> target_streams;
	
	
	@XmlElement(name = "parameters")
	@ApiModelProperty(value = "Parameter definitions", required = true)
	@NotNull
	public List<TemplateParameter> parameters;
	
	public TemplateParameterDefinition() {}
}
