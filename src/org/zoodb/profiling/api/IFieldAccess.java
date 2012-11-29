package org.zoodb.profiling.api;

import java.io.Serializable;

/**
 * Information object for field access
 * 
 * holds tuple: (trxId,oid,fieldname,{r,w},active,bytes)
 * @author tobiasg
 *
 */
public interface IFieldAccess extends Serializable {
	
	public Class getAssocClass();
	
	public void setClass(Class c);
	
	
	public long getOid();
	
	public void setOid(long oid);
	
	
	public String getUniqueTrxId();

	public void setUniqueTrxId(String uniqueTrxId);
	
	
	public String getFieldName();
	
	public void setFieldName(String fieldName);
	
	
	public boolean isWrite();
	
	public void setWrite(boolean write);
	
	
	public boolean isActive();
	
	public void setActive(boolean active);
	
	
	public long sizeInBytes();
	
	public void setSizeInBytes(long bytes);
	
	
	public long getTimestamp();
	
	public void setTimestamp(long timestamp);
}