package edu.ucla.nesl.sensorsafe.model;

import java.util.LinkedList;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Rule definition.")
@XmlRootElement(name = "rule")
public class Rule {

	@XmlElement(name = "targetUsers")
	@ApiModelProperty(value = "List of user names for this rule to be applied. If null, always applied.")
	public List<String> targetUsers;
	
	@XmlElement(name = "targetStreams")
	@ApiModelProperty(value = "List of stream names for this rule to be applied. If null, applied to all streams.")
	public List<String> targetStreams;
	
	@XmlElement(name = "condition")
	@ApiModelProperty(value = "Rule condition.", required = true)
	@NotNull
	public String condition;
	
	@XmlElement(name = "action")
	@ApiModelProperty(value = "Rule action..", required = true, allowableValues = "allow,deny")
	@NotNull
	public String action;
	
	public Rule() {}
	
	public Rule(Object[] targetUsers, Object[] targetStreams, String condition, String action) {
		if (targetUsers != null)  {
			this.targetUsers = new LinkedList<String>();
			for (Object user: targetUsers) {
				this.targetUsers.add((String)user);
			}
		}
		if (targetStreams != null)  {
			this.targetStreams = new LinkedList<String>();
			for (Object stream: targetStreams) {
				this.targetStreams.add((String)stream);
			}
		}
		this.condition = condition;
		this.action = action;
	}
}
