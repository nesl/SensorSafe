package edu.ucla.nesl.sensorsafe.model;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RuleCollection {
	public List<Rule> rules;
	
	public RuleCollection() {}
	
	public RuleCollection(List<Rule> rules) {
		this.rules = rules != null ? new LinkedList<Rule>(rules) : null; 
	}
}
