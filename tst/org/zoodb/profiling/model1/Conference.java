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
public class Conference extends PersistenceCapableImpl     {	

	private String key;
	
	private String Location;
	
	private int year;

	private String issue;

    private List<Publication> publications;
    
    private ConferenceSeries series;

   
    public Conference() {
    	publications = new LinkedList<Publication>();
    }
    
    
	public List<Publication> getPublications() {
		activateRead("publications");
	  	return this.publications;
	}
		
	public void setPublications(List<Publication> publications) throws Exception{		
		activateWrite("publications");
		if(publications != null && publications.size() < 1){
			throw new Exception("Constraint violation: publications must have size of at least 1");
		}
		this.publications = publications;
	}
	
	public void addPublications(Publication publication) throws Exception{
		activateWrite("publications");
		if (this.publications == null) {
			throw new Exception(
					"Association publications is not initialized yet. Please use setPublications(List<Publication>) instead");
		}
		if(publication != null){
			this.publications.add(publication);
		}		
	}
			

	public ConferenceSeries getSeries()  {
		activateRead("series");
	  	return this.series;
	}
	public void setSeries(ConferenceSeries series) {
		activateWrite("series");
	  	this.series = series;
	}
    public String getLocation() {
		activateRead("Location");
	  	return this.Location;
	}
    public void setLocation(String Location) {
		activateWrite("Location");
		this.Location = Location;
	}
    public int getYear() {
		activateRead("year");
	  	return this.year;
	}
    public void setYear(int year) {
		activateWrite("year");
		this.year = year;
	}
    public String getIssue() {
		activateRead("issue");
	  	return this.issue;
	}
    public void setIssue(String issue) {
		activateWrite("issue");
		this.issue = issue;
	}
    public String getKey() {
    	activateRead("key");
		return key;
	}
	public void setKey(String key) {
		activateWrite("key");
		this.key = key;
	}
	public void addPublication(Publication p1) {
		activateWrite("publications");
		publications.add(p1);
	}
}
