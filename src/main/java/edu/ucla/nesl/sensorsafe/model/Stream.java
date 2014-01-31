package edu.ucla.nesl.sensorsafe.model;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.LinkedList;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "Stream definition.")
@XmlRootElement
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
	
	public Stream(int id, String name, String owner, String tags, Array channels) throws SQLException {
		this.id = id;
		this.name = name;
		this.owner = owner;
		this.tags = tags;
		this.channels = getListChannelFromSqlArray(channels);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((owner == null) ? 0 : owner.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Stream other = (Stream) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (owner == null) {
			if (other.owner != null)
				return false;
		} else if (!owner.equals(other.owner))
			return false;
		return true;
	}

	@XmlTransient
	public String getVirtualTableName() {
		return getChannelFormatPrefix() + "vtable";
	}
	
	@XmlTransient
	public String getStreamTableName() {
		return getChannelFormatPrefix() + "streams";
	}

	@XmlTransient
	public static String getChannelFormatPrefix(List<Channel> channels) {
		String prefix = "";
		for (Channel channel: channels) {
			prefix += channel.type + "_";
		}
		return prefix;
	}

	@XmlTransient
	public String getChannelFormatPrefix() {
		return getChannelFormatPrefix(channels);
	}

	@XmlTransient
	public String getRowTypeName() {
		return getChannelFormatPrefix() + "rowtype";
	}

	@XmlTransient
	public static String getRowTypeName(List<Channel> channel) {
		return getChannelFormatPrefix(channel) + "rowtype";
	}

	@XmlTransient
	public static List<Channel> getListChannelFromSqlArray(Array sqlArray) throws SQLException {
		List<Channel> channels = new LinkedList<Channel>();
		Object[] objArray = (Object[])sqlArray.getArray();
		for (Object obj: objArray) {
			Struct struct = (Struct)obj;
			Object[] attr = struct.getAttributes();
			channels.add(new Channel((String)attr[0], (String)attr[1]));
		}
		return channels;
	}

	@XmlTransient
	public List<Channel> getChannels() {
		return channels;
	}

	public void setStreamId(int id) {
		this.id = id;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	@XmlTransient
	public boolean isChannelsValid() {
		for (Channel channel: channels) {
			if (!channel.type.equals("float")
					&& !channel.type.equals("int")
					&& !channel.type.equals("text")) {
				return false;
			}
		}
		return true;
	}

	public void setNumSamples(int numSamples) {
		this.num_samples = numSamples;
	}

	@XmlTransient
	public String getChannelsAsSqlList() {
		String ret = "LIST{";
		for (Channel ch: channels) {
			String str = "ROW('" + ch.name + "', '" + ch.type + "')";
			ret += str + ", ";
		}
		ret = ret.substring(0, ret.length() -2);
		ret += "}";
		return ret;
	}
}	
