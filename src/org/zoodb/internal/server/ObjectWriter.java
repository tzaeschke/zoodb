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
package org.zoodb.internal.server;


import org.zoodb.internal.SerialOutput;

/**
 * This class serves as a mediator between the serializer and the file access class.
 * Compared to the StorageWrite, the ObjectWriter also provides the following:
 * - Updating the oid- and class-index with new object positions.
 * - Insert the page header (currently containing only the class-oid).
 * 
 * @author Tilmann Zaeschke
 */
public interface ObjectWriter extends SerialOutput {

	public void startObject(long oid, int prevSchemaVersion);

	public void finishObject();

	public void flush();
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	public void newPage();
	
}
