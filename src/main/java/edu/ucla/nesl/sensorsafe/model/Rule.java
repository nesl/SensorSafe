package edu.ucla.nesl.sensorsafe.model;

import java.util.LinkedList;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Rule")
@XmlRootElement(name = "rule")
public class Rule {
	
	@XmlElement(name = "id")
	@ApiModelProperty(value = "Unique id for this rule.")
	public int id;
	
	@XmlElement(name = "priority")
	public int priority = Integer.MAX_VALUE;
	
	@XmlElement(name = "target_users")
	@ApiModelProperty(value = "List of user names for this rule to be applied. If null, always applied.")
	public List<String> targetUsers;
	
	@XmlElement(name = "target_streams")
	@ApiModelProperty(value = "List of stream names for this rule to be applied. If null, applied to all streams.")
	public List<String> targetStreams;
	
	@XmlElement(name = "condition")
	@ApiModelProperty(value = "Rule condition.")
	@NotNull
	public String condition;
	
	@XmlElement(name = "action")
	@ApiModelProperty(value = "Rule action..", required = true, allowableValues = "allow,deny")
	@NotNull
	public String action;
	
	public Rule() {}
	
	public Rule(int id, Object[] targetUsers, Object[] targetStreams, String condition, String action, int priority) {
		this.id = id;
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
		this.priority = priority;
	}
}
