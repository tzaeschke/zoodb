package org.zoodb.profiling;

/**
 * @author tobiasg
 * 
 * This class serves as a transfer object. 
 *
 */
public class FieldAccess {
	
	private String fieldName;
	private boolean isWriteAccess;
	private String objectId;
	private String className;

	public FieldAccess(String fieldName, boolean isWriteAccess, String objectId, String className) {
		this.fieldName = fieldName;
		this.isWriteAccess = isWriteAccess;
		this.objectId = objectId;
		this.className = className;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public boolean isWriteAccess() {
		return isWriteAccess;
	}

	public void setWriteAccess(boolean isWriteAccess) {
		this.isWriteAccess = isWriteAccess;
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}
	
}
