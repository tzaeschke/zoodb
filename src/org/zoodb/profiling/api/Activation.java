package org.zoodb.profiling.api;

import java.lang.reflect.Field;

import org.zoodb.api.impl.ZooPCImpl;

public class Activation {
	
	private ZooPCImpl activator;
	private String memberName;
	private Object memberResult;
	private Field field;
	
	public Activation(ZooPCImpl activator, String memberName, Object memberResult, Field field) {
		this.activator = activator;
		this.memberName = memberName;
		this.memberResult = memberResult;
		this.field = field;
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
	public Object getMemberResult() {
		return memberResult;
	}
	public void setMemberResult(ZooPCImpl memberResult) {
		this.memberResult = memberResult;
	}
	
	public String prettyString() {
		StringBuilder sb = new StringBuilder();
		
		if (activator != null) {
			sb.append("ACTIVATOR_Class:");
			sb.append(activator.getClass().getName());
			
			sb.append(":ACTIVATOR_Ref:");
			sb.append(activator.hashCode());
			
			sb.append(":ACTIVATOR_OID:");
			sb.append(activator.jdoZooGetOid());
		}
		if (memberName != null) {
			sb.append(":MEMBER:");
			sb.append(this.memberName);
		}
		if (memberResult != null) {
			sb.append(":TARGETREF_Class:");
			sb.append(memberResult.getClass().getName());
			
			sb.append("TARGETREF_Ref:");
			sb.append(memberResult.hashCode());
			
			try {
				ZooPCImpl target = (ZooPCImpl) memberResult;
				sb.append("TARGETREF_OID");
				sb.append(target.jdoZooGetOid());
			} catch (ClassCastException e) {
				//TODO: special behaviour for non-DB-classes?
			}
		}
		
		return sb.toString();
	}
	
	public String getTargetOid() {
		return (memberResult instanceof ZooPCImpl) ? String.valueOf( ((ZooPCImpl) memberResult).jdoZooGetOid() ) : null;
	}
	
	public String getOid() {
		return String.valueOf(activator.jdoZooGetOid());
	}
	
	@Override
	public boolean equals(Object a) {
		Activation ac = (Activation) a;
		
		boolean sameClass = ac.getActivator().getClass().getName().equals(this.getActivator().getClass().getName());
		boolean sameRef = ac.getActivator().hashCode() == this.getActivator().hashCode();
		boolean sameOid = ac.getActivator().jdoZooGetOid() == this.getActivator().jdoZooGetOid();
		boolean sameMember = this.memberName.equals(ac.getMemberName()); 
		boolean sameResult = this.memberResult.getClass().getName().equals(ac.getMemberResult().getClass().getName());
		//compares references: different trx --> different references
		boolean sameResultRef = this.memberResult.hashCode() == ac.getMemberResult().hashCode(); 
		
		return sameClass && sameRef && sameOid && sameMember && sameResult && sameResultRef;
		
	}

	public Field getField() {
		return field;
	}

	public void setField(Field field) {
		this.field = field;
	}
	
	
}
