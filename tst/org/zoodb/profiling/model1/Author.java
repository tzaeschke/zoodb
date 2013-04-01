/* 
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) This class was generated using the OMS plugin
 */
package org.zoodb.profiling.model1;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * @author tobiasg
 * @version 1.0 
 */
public class Author extends PersistenceCapableImpl     {	

	private String Name;
//outcomment the following line when in the unoptimized model
	private AuthorContact contact;
	
	private List<Publication> sourceA;
	
//outcomment the following 3 lines when in the optimized model
//	private String email;
//	private String university;
//	private int aggregatedRating;
	
	
	public Author() {
		sourceA = new LinkedList<Publication>();
	}
	
	public List<Publication> getSourceA() {
		activateRead("sourceA");
	  	return this.sourceA;
	}	
	
	public void setSourceA(List<Publication> sourceA) {
		activateWrite("sourceA");
	  	this.sourceA = sourceA;
	}
    public String getName() {
		activateRead("Name");
	  	return this.Name;
	}
    public void setName(String Name) {
		activateWrite("Name");
		this.Name = Name;
	}
    public void addPublication(Publication p) {
    	activateWrite("sourceA"); 
    	sourceA.add(p);
    }
// outcomment the following 2 methods when in the unoptimized model
    public AuthorContact getDetails() {
    	activateRead("contact");
		return contact;
	}
	public void setDetails(AuthorContact contact) {
		activateWrite("contact");
		this.contact = contact;
	}
//outcomment all the lines below when in the optimized model
//	public int getAggregatedRating() {
//		activateRead("aggregatedRating");
//		return aggregatedRating;
//	}
//	public void setAggregatedRating(int aggregatedRating) {
//		activateWrite("aggregatedRating");
//		this.aggregatedRating = aggregatedRating;
//	}
//	public String getEmail() {
//		activateRead("email");
//		return email;
//	}
//	public void setEmail(String email) {
//		activateWrite("email");
//		this.email = email;
//	}
//	public String getUniversity() {
//		activateRead("university");
//		return university;
//	}
//	public void setUniversity(String university) {
//		activateWrite("university");
//		this.university = university;
//	}
	
	

}