package edu.ucla.nesl.sensorsafe.model;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class StreamCollection {
	public List<Stream> streams;
	
	public StreamCollection() {}
	
	public StreamCollection(List<Stream> streams) {
		this.streams = (streams != null) ? new LinkedList<Stream>(streams) : null;
	}
}
