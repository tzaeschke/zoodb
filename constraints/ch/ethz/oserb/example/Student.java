package ch.ethz.oserb.example;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.PrimaryKey;
import net.sf.oval.constraint.Unique;

@PrimaryKey(keys="studentID")
public class Student extends ZooPCImpl {
	private int studentID;
	private String name;

	public Student(int studentID, String name) {
		this.studentID = studentID;
		this.name = name;
	}

	public Student() {
		
	}

	public void setStudentID(int studentID) {
		this.studentID = studentID;
	}

	public int getStudentID() {
		return studentID;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

}
