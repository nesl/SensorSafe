package edu.ucla.nesl.sensorsafe.model;

import java.util.HashSet;
import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringUtils;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import edu.ucla.nesl.sensorsafe.db.informix.Aggregator;

@ApiModel(value = "Rule")
@XmlRootElement
public class Rule {

	private static final String[] VALID_RULE_ACTIONS = { "allow", "deny" }; 

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
	
	@XmlElement(name = "tags")
	public String tags;
	
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

	public boolean isValidRule() {
		boolean isAggregator = false;

		// Check action field
		if (action == null) {
			throw new IllegalArgumentException("Rule action field cannot be null.");
		}

		boolean isValid = false;
		for (String validAction: VALID_RULE_ACTIONS) {
			if (action.equalsIgnoreCase(validAction)) {
				isValid = true;
			}
		}
		if (!isValid) {
			// Check if this is valid aggregate rule.
			if (Aggregator.isAggregateExpression(action)) {
				isAggregator = true;
				if (condition != null) {
					throw new IllegalArgumentException("Aggregate rules with filtering condition is not supported.");
				}
				if (priority != Integer.MAX_VALUE) {
					throw new IllegalArgumentException("Priority for aggregate rule should not be specified.");
				}
			} else {
				throw new IllegalArgumentException("Invalid rule action. Valid actions are: " + StringUtils.join(VALID_RULE_ACTIONS, ", "));
			}
		}
		isValid = false;
		if (priority < 1) {
			throw new IllegalArgumentException("priority must be greater than or equal to 1.");
		}

		return isAggregator;
	}
}
