package edu.ucla.nesl.sensorsafe.model;

import java.util.HashSet;
import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Rule")
@XmlRootElement(name = "rule")
public class Rule {
	
	@XmlElement(name = "id")
	public int id;
	
	@XmlElement(name = "priority")
	public int priority = Integer.MAX_VALUE;
	
	@XmlElement(name = "target_users")
	@ApiModelProperty(value = "List of user names for this rule to be applied. If null, always applied.")
	public Set<String> target_users;
	
	@XmlElement(name = "target_streams")
	@ApiModelProperty(value = "List of stream names for this rule to be applied. If null, applied to all streams.")
	public Set<String> target_streams;
	
	@XmlElement(name = "condition")
	public String condition;
	
	@XmlElement(name = "action")
	@ApiModelProperty(required = true)
	@NotNull
	public String action;
	
	@XmlElement(name = "template_name")
	public String template_name;
	
	public Rule() {}
	
	public Rule(int id, Object[] targetUsers, Object[] targetStreams, String condition, String action, int priority) {
		this.id = id;
		if (targetUsers != null)  {
			this.target_users = new HashSet<String>();
			for (Object user: targetUsers) {
				this.target_users.add((String)user);
			}
		}
		if (targetStreams != null)  {
			this.target_streams = new HashSet<String>();
			for (Object stream: targetStreams) {
				this.target_streams.add((String)stream);
			}
		}
		this.condition = condition;
		this.action = action;
		this.priority = priority;
	}
	
	public Rule(int id, Object[] targetUsers, Object[] targetStreams, String condition, String action, int priority, String templateName) {
		this(id, targetUsers, targetStreams, condition, action, priority);
		this.template_name = templateName;
	}
}
