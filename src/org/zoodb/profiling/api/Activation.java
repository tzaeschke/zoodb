package org.zoodb.profiling.api;

import java.lang.reflect.Field;

import org.zoodb.api.impl.ZooPCImpl;

public class Activation {
	
	private ZooPCImpl activator;
	private String memberName;
	private Field field;
	private long totalObjectBytes;
	private String memberResultOid;
	private Class<?> memberResultClass;
	
	public Activation(ZooPCImpl activator, String memberName, Class<?> memberResultClass,String memberResultOid, Field field) {
		this.activator = activator;
		this.memberName = memberName;
		this.field = field;
		this.memberResultClass = memberResultClass;
		this.memberResultOid = memberResultOid;
	}
	
	public ZooPCImpl getActivator() {
		return activator;
	}
	public void setActivator(ZooPCImpl activator) {
		this.activator = activator;
	}
	public String getMemberName() {
		return memberName;
	}
	public void setMemberName(String memberName) {
		this.memberName = memberName;
	}
	
	
	public Class<?> getMemberResultClass() {
		return memberResultClass;
	}
	
	
	
	public String prettyString() {
		StringBuilder sb = new StringBuilder();
		
		if (activator != null) {
			sb.append("ACTIVATOR_Class:");
			sb.append(activator.getClass().getName());
			
			sb.append(":ACTIVATOR_OID:");
			sb.append(activator.jdoZooGetOid());
			
			sb.append(":BYTES(R):");
			sb.append(totalObjectBytes);
		}
		if (memberName != null) {
			sb.append(":MEMBER:");
			sb.append(this.memberName);
		}
		
		sb.append(":TARGETREF_Class:");
		sb.append(memberResultClass.getName());
		
		if (memberResultOid != null) {
			sb.append("TARGETREF_OID");
			sb.append(memberResultOid);
		}
	
		
		return sb.toString();
	}
	
	public String getTargetOid() {
		return memberResultOid;
	}
	
	public String getOid() {
		return String.valueOf(activator.jdoZooGetOid());
	}
	
	@Override
	public boolean equals(Object a) {
		Activation ac = (Activation) a;
		
		boolean sameClass = ac.getActivator().getClass().getName().equals(this.getActivator().getClass().getName());
		boolean sameOid = ac.getActivator().jdoZooGetOid() == this.getActivator().jdoZooGetOid();
		boolean sameMember = this.memberName.equals(ac.getMemberName()); 
		boolean sameResult = this.memberResultClass == ac.getMemberResultClass();
		 
		return sameClass && sameOid && sameMember && sameResult;
		
	}

	public Field getField() {
		return field;
	}

	public void setField(Field field) {
		this.field = field;
	}

	public long getTotalObjectBytes() {
		return totalObjectBytes;
	}

	public void setTotalObjectBytes(long totalObjectBytes) {
		this.totalObjectBytes = totalObjectBytes;
	}	
	
}
