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

	long getOid();

	void setOid(long oid);

	byte getAttrByte(String attrName);

	boolean getAttrBool(String attrName);

	short getAttrShort(String attrName);

	int getAttrInt(String attrName);

	long getAttrLong(String attrName);

	char getAttrChar(String attrName);

	float getAttrFloat(String attrName);

	double getAttrDouble(String attrName);

	String getAttrString(String attrName);

	Date getAttrDate(String attrName);

	ZooHandle getAttrRefHandle(String attrName);

	long getAttrRefOid(String attrName);

	void remove();
	
	ZooClass getType();
	
	/**
	 * 
	 * @return a java instance of the object. This will fail if the schema of the referenced
	 * instance does not match the Java class in the current classpath or if there is no such
	 * Java class.
	 */
	Object getJavaObject();

	/**
	 * 
	 * @param fieldName The name of the field whose value should be returned
	 * @return The value of the field or {@code null} if the field could not be found.
	 */
	Object getValue(String fieldName);

	void setValue(String attrName, Object val);
	
}