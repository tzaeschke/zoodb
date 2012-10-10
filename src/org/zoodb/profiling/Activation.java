package org.zoodb.profiling;

import org.zoodb.api.impl.ZooPCImpl;

public class Activation {
	
	private ZooPCImpl activator;
	private String memberName;
	private Object memberResult;
	
	public Activation(ZooPCImpl activator, String memberName, Object memberResult) {
		this.activator = activator;
		this.memberName = memberName;
		this.memberResult = memberResult;
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
	
}
