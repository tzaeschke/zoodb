package org.zoodb.profiling.model2;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class AuthorContact extends PersistenceCapableImpl {
	
	private String email;
	private String university;
	
	public AuthorContact() {
		
	}

	public String getEmail() {
		activateRead("email");
		return email;
	}
	public void setEmail(String email) {
		activateWrite("email");
		this.email = email;
	}
	public String getUniversity() {
		activateRead("university");
		return university;
	}
	public void setUniversity(String university) {
		activateWrite("university");
		this.university = university;
	}
}