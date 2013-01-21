/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo.internal;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.api.ZooField;

public class ISchemaField extends ZooField {

	private boolean isInvalid = false;
	private ZooFieldDef fieldDef;

	ISchemaField(ZooFieldDef fieldDef) {
		this.fieldDef = fieldDef;
	}

	@Override
	public String toString() {
		checkInvalid();
		return "Class schema field: " + fieldDef.getName();
	}

	private void checkInvalid() {
		System.err.println("FIXME: Check ionvalid field instance.");
		if (isInvalid) {
			throw new JDOUserException("This schema field object is invalid, for " +
					"example because it has been deleted.");
		}
	}

	@Override
	public void remove() {
		checkInvalid();
		isInvalid = true;
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rename(String name) {
		checkInvalid();
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getFieldName() {
		checkInvalid();
		return fieldDef.getName();
	}


	
}
