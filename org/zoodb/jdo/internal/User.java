package org.zoodb.jdo.internal;

public class User {

	private final String name;
	private String password;
	private boolean isPasswordRequired;
	private boolean isDBA;
	private boolean isRW;
	
	public User(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public boolean isPasswordRequired() {
		return isPasswordRequired;
	}
	public void setPasswordRequired(boolean isPasswordRequired) {
		this.isPasswordRequired = isPasswordRequired;
	}
	public boolean isDBA() {
		return isDBA;
	}
	public void setDBA(boolean isDBA) {
		this.isDBA = isDBA;
	}
	public boolean isRW() {
		return isRW;
	}
	public void setRW(boolean isRW) {
		this.isRW = isRW;
	}
	
}
