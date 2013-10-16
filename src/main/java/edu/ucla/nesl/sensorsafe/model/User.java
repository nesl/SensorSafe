package edu.ucla.nesl.sensorsafe.model;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

@ApiModel(value = "User information.")
@XmlRootElement(name = "user")
public class User {

	@XmlElement(name = "email")
	@ApiModelProperty(value = "E-mail", required = true)
	@NotNull
	public String email;

	@XmlElement(name = "group")
	@ApiModelProperty(value = "Group", allowableValues = "owner,user,admin", required = true)
	@NotNull
	public String group;
	
	@XmlElement(name = "password")
	@ApiModelProperty(value = "Password", required = true)
	@NotNull
	public String password;
	
	@XmlElement(name = "firstName")
	@ApiModelProperty(value = "First name")
	public String firstName;
	
	@XmlElement(name = "lastName")
	@ApiModelProperty(value = "Last name")
	public String lastName;

	@XmlElement(name = "apikey")
	@ApiModelProperty(value = "Apikey")
	public String apikey;
	
	public User() {}
	
	public User(String email, String firstName, String lastName) {
		this.email = email;
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	public User(String email, String group, String firstName, String lastName, String apikey) {
		this.email = email;
		this.group = group;
		this.apikey = apikey;
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	public void setApikey(String apikey) {
		this.apikey = apikey;
	}
	
	public void setGroup(String group) {
		this.group = group;
	}
}
