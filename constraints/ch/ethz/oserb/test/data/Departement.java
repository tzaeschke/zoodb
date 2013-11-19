package ch.ethz.oserb.test.data;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.PrimaryKey;
import net.sf.oval.constraint.Unique;

@PrimaryKey(keys="deptID", profiles="primary")
public class Departement extends ZooPCImpl {

	private int deptID;
	private String name;
	
	public Departement(int deptID, String name) {
		this.deptID = deptID;
		this.name = name;
	}
	public int getDeptID() {
		return deptID;
	}
	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
