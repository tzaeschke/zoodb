package org.zoodb.profiling.api;

import java.lang.reflect.Field;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * This class servers as a transfer object
 * @author tobiasg
 *
 */
public class Activation {
	
	private String triggerName;
	private Field field;
	private long totalObjectBytes;
	
	private long predecessorOid;
	private long activatorOid;
	private long targetOid;
	
	private Class<?> activatorClass;
	private Class<?> targetClass;
	
	public Activation(Class<?> activatorClass,long activatorOid ,String memberName, Class<?> memberResultClass,long memberResultOid, Field field, long predecessorOid) {
		this.activatorClass = activatorClass;
		this.triggerName = memberName;
		this.field = field;
		this.targetClass = memberResultClass;
		this.targetOid = memberResultOid;
		this.activatorOid = activatorOid;
		this.predecessorOid = predecessorOid;
	}
	
	public Class<?> getActivatorClass() {
		return activatorClass;
	}
	public void setActivator(Class<?> activatorClass) {
		this.activatorClass = activatorClass;
	}
	public String getMemberName() {
		return triggerName;
	}
	public void setMemberName(String memberName) {
		this.triggerName = memberName;
	}
	
	
	public Class<?> getMemberResultClass() {
		return targetClass;
	}
	
	
	public String prettyString() {
		StringBuilder sb = new StringBuilder();
		
		if (activatorClass != null) {
			sb.append("ACTIVATOR_Class:");
			sb.append(activatorClass.getName());
			
			sb.append(":ACTIVATOR_OID:");
			sb.append(activatorOid);
			
			sb.append(":BYTES(R):");
			sb.append(totalObjectBytes);
		}
		if (triggerName != null) {
			sb.append(":MEMBER:");
			sb.append(triggerName);
		}
		
		sb.append(":TARGETREF_Class:");
		sb.append(targetClass.getName());
		
		if (targetOid != 0) {
			sb.append("TARGETREF_OID");
			sb.append(targetOid);
		}
	
		
		return sb.toString();
	}
	
	public long getTargetOid() {
		return targetOid;
	}
	
	
	
	@Override
	public boolean equals(Object a) {
		Activation ac = (Activation) a;
		
		boolean sameClass = ac.getActivatorClass().equals(this.activatorClass);
		boolean sameOid = ac.getActivatorOid() == this.activatorOid;
		boolean sameMember = this.triggerName.equals(ac.getMemberName()); 
		boolean sameResult = this.targetClass == ac.getMemberResultClass();
		 
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

	public long getActivatorOid() {
		return activatorOid;
	}

	public void setActivatorOid(long activatorOid) {
		this.activatorOid = activatorOid;
	}

	public long getPredecessorOid() {
		return predecessorOid;
	}

	public void setPredecessorOid(long predecessorOid) {
		this.predecessorOid = predecessorOid;
	}
	
}
