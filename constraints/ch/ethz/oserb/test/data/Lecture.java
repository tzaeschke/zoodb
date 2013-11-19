package ch.ethz.oserb.test.data;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.PrimaryKey;
import net.sf.oval.constraint.ForeignKey;
import net.sf.oval.constraint.Unique;

@Unique(attr="title", profiles="unique")
//@PrimaryKey(keys="lectureID")
public class Lecture extends ZooPCImpl{
	
	private int lecutreID;
	private String title;
	private int credits;
	@ForeignKey(clazz = Prof.class, attr="profID", profiles="foreign")
	private int profID;
	

	public Lecture(int lecutreID, String title, int credits, int profID) {
		this.lecutreID = lecutreID;
		this.title = title;
		this.credits = credits;
		this.profID = profID;
	}
	public int getLecutreID() {
		return lecutreID;
	}
	public void setLecutreID(int lecutreID) {
		this.lecutreID = lecutreID;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public int getCredits() {
		return credits;
	}
	public void setCredits(int credits) {
		this.credits = credits;
	}
	public int getProfID() {
		return profID;
	}
	public void setProfID(int profID) {
		this.profID = profID;
	}
	
	
}
