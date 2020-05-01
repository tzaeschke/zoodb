/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.schema;

import java.util.Date;

public interface ZooHandle {

	public abstract long getOid();

	public abstract void setOid(long oid);

	public abstract byte getAttrByte(String attrName);

	public abstract boolean getAttrBool(String attrName);

	public abstract short getAttrShort(String attrName);

	public abstract int getAttrInt(String attrName);

	public abstract long getAttrLong(String attrName);

	public abstract char getAttrChar(String attrName);

	public abstract float getAttrFloat(String attrName);

	public abstract double getAttrDouble(String attrName);

	public abstract String getAttrString(String attrName);

	public abstract Date getAttrDate(String attrName);

	public abstract ZooHandle getAttrRefHandle(String attrName);

	public abstract long getAttrRefOid(String attrName);

	public abstract void remove();
	
	public abstract ZooClass getType();
	
	/**
	 * 
	 * @return a java instance of the object. This will fail if the schema of the referenced
	 * instance does not match the Java class in the current classpath or if there is no such
	 * Java class.
	 */
	public abstract Object getJavaObject();

	/**
	 * 
	 * @param fieldName The name of the field whose value should be returned
	 * @return The value of the field or {@code null} if the field could not be found.
	 */
	public abstract Object getValue(String fieldName);

	public abstract void setValue(String attrName, Object val);
	
}