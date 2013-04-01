/* 
 * Copyright 2000-2011 ETH Zurich. All Rights Reserved.
 *
 * This software is proprietary information of ETH Zurich.
 * Use is subject to license terms.
 *
 * @(#) This class was generated using the OMS plugin
 */
package org.zoodb.profiling.model2;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * @author tobiasg
 * @version 1.0 
 */
public class Publication extends PersistenceCapableImpl     {	

	private Conference conference;

	private int Year;

	private String Title;
	
	private String key;
	
	private int rating;
//outcomment the following 3 lines when in the unoptimized model
//	private int downloadCount;
//	private int citationCount;
//	private String Abstract;

//outcomment the following 3 lines when in the optimized model
	private PublicationSplit ps;
	private PublicationAbstract pAbstract;
    private String conferenceIssue;	

	
    private List<Author> targetA;
    
    private List<Tags> targetT;
    


    public Publication() {
    	targetT = new LinkedList<Tags>();
    }
    
	public List<Author> getTargetA() {
		activateRead("targetA");
	  	return this.targetA;
	}
	public void setTargetA(List<Author> targetA) throws Exception{		
		activateWrite("targetA");
		if(targetA != null && targetA.size() < 1){
			throw new Exception("Constraint violation: targetA must have size of at least 1");
		}
		this.targetA = targetA;
	}
	public void addTargetA(Author author) throws Exception{
		activateWrite("targetA");
		if (this.targetA == null) {
			throw new Exception(
					"Association targetA is not initialized yet. Please use setTargetA(List<Author>) instead");
		}
		if(author != null){
			this.targetA.add(author);
		}		
	}

	public List<Tags> getTargetT() {
		activateRead("targetT");
	  	return this.targetT;
	}
	public void setTargetT(List<Tags> targetT) {		
		activateWrite("targetT");
		this.targetT = targetT;
	}
	public void addTag(Tags t) {
		activateWrite("targetT");
		this.targetT.add(t);
	}
	
	
	public void addTargetT(Tags tags) throws Exception{
		activateWrite("targetT");
		if (this.targetT == null) {
			throw new Exception(
					"Association targetT is not initialized yet. Please use setTargetT(List<Tags>) instead");
		}
		if(tags != null){
			this.targetT.add(tags);
		}		
	}
			
	
    public Conference getConference() {
		activateRead("conference");
	  	return this.conference;
	}
    public void setConference(Conference conference) {
		activateWrite("conference");
		this.conference = conference;
	}
    public int getYear() {
		activateRead("Year");
	  	return this.Year;
	}
    public void setYear(int Year) {
		activateWrite("Year");
		this.Year = Year;
	}
//outcomment the following 2 methods when in the unoptimized model
//    public String getAbstract() {
//		activateRead("Abstract");
//	  	return this.Abstract;
//	}
//    public void setAbstract(String Abstract) {
//		activateWrite("Abstract");
//		this.Abstract = Abstract;
//	}
    public String getTitle() {
		activateRead("Title");
	  	return this.Title;
	}
    public void setTitle(String Title) {
		activateWrite("Title");
		this.Title = Title;
	}
    public int getRating() {
    	activateRead("rating");
		return rating;
	}
	public void setRating(int rating) {
		activateWrite("rating");
		this.rating = rating;
	}
	public String getKey() {
		activateRead("key");
		return key;
	}
	public void setKey(String key) {
		activateWrite("key");
		this.key = key;
	}
//outcomment the following 6 methods
    public PublicationAbstract getpAbstract() {
    	activateRead("pAbstract");
		return pAbstract;
	}
	public void setpAbstract(PublicationAbstract pAbstract) {
		activateWrite("pAbstract");
		this.pAbstract = pAbstract;
	}
	public PublicationSplit getPs() {
		activateRead("ps");
		return ps;
	}
	public void setPs(PublicationSplit ps) {
		activateWrite("ps");
		this.ps = ps;
	}
    public String getConferenceIssue() {
    	activateRead("conferenceIssue");
		return conferenceIssue;
	}
	public void setConferenceIssue(String conferenceIssue) {
		activateWrite("conferenceIssue");
		this.conferenceIssue = conferenceIssue;
	}
	
	
//outcomment the following 4 methods when in the unoptimized model
//	public int getDownloadCount() {
//		activateRead("downloadCount");
//		return downloadCount;
//	}
//	public void setDownloadCount(int downloadCount) {
//		activateWrite("downloadCount");
//		this.downloadCount = downloadCount;
//	}
//	public int getCitationCount() {
//		activateRead("citationCount");
//		return citationCount;
//	}
//	public void setCitationCount(int citationCount) {
//		activateWrite("citationCount");
//		this.citationCount = citationCount;
//	}
}
