package edu.ucla.nesl.sensorsafe.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;

@ApiModel(value = "Channel statistics.")
@XmlRootElement
public class Statistics {
	
	@XmlElement(name = "min")
	public double min;
	
	@XmlElement(name = "max")
	public double max;
	
	public Statistics() {}
	
	public Statistics(double min, double max) {
		this.min = min;
		this.max = max;
	}
}
