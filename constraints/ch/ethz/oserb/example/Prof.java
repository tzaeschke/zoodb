package ch.ethz.oserb.example;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.PrimaryKey;
import net.sf.oval.constraint.ForeignKey;;

@PrimaryKey(keys="profID")
public class Prof extends ZooPCImpl {
	
	private int profID;
	private String name;
	@ForeignKey(clazz=Departement.class, attr = "deptID")
	private int deptID;
	
	public Prof(int profID, String name, int deptID) {
		this.profID = profID;
		this.name = name;
		this.deptID = deptID;
	}
	public int getProfID() {
		return profID;
	}
	public void setProfID(int profID) {
		this.profID = profID;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getDeptID() {
		return deptID;
	}
	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}
}
