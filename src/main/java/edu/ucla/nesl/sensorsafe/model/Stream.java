package edu.ucla.nesl.sensorsafe.model;

import java.util.LinkedList;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Stream definition.")
@XmlRootElement(name = "stream")
public class Stream {
	@XmlElement(name = "name")
	@ApiModelProperty(value = "Stream name", required = true)
	@NotNull
	public String name;

	@XmlElement(name = "owner")
	@ApiModelProperty(value = "Stream owner", required = true)
	public String owner;

	@XmlElement(name = "channels")
	@ApiModelProperty(value = "channel definitions", required = true)
	@NotNull
	public List<Channel> channels;
	
	@XmlElement(name = "tags")
	@ApiModelProperty(value = "tags")
	public String tags;
	
	@XmlElement(name = "id")
	@ApiModelProperty(value = "ID")
	public int id;
	
	@XmlElement(name = "num_samples")
	@ApiModelProperty(value = "Total number of samples in the stream")
	public long num_samples;
	
	public Stream() {}
	
	public Stream(int id, String name, String owner, String tags, List<Channel> channels, long numSamples) {
		this.id = id;
		this.name = name;
		this.owner = owner;
		this.tags = tags;
		this.channels = (channels != null) ? new LinkedList<Channel>(channels) : null;
		this.num_samples = numSamples;
	}
}	
