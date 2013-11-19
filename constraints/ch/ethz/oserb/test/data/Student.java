package ch.ethz.oserb.test.data;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.OclConstraint;
import net.sf.oval.constraint.OclConstraints;
import net.sf.oval.constraint.PrimaryKey;
import net.sf.oval.constraint.Unique;

@PrimaryKey(keys="studentID", profiles="primary")
@OclConstraints(@OclConstraint(expr="context Student inv: self.age>18", profiles="oclAnnotation"))
public class Student extends ZooPCImpl {
	
	private int studentID;
	private String name;
	private int age;
	
	public Student(int studentID, String name, int age) {
		this.studentID = studentID;
		this.name = name;
		this.age = age;
	}

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

	public int getAge() {
		return age;
	}
	
	public void setAge(int age) {
		this.age = age;
	}
}
