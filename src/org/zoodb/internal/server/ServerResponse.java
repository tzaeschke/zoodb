/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.server;

import org.zoodb.internal.util.DBLogger;

/**
 * 
 * @author Tilmann Zäschke
 *
 */
public class ServerResponse {

	public static enum RESULT {
		SUCCESS,
		OBJECT_NOT_FOUND;
	}
	
	private final RESULT result;
	private final String message;
	
	public ServerResponse(RESULT error) {
		this(error, null);
	}
	
	public ServerResponse(RESULT error, String message) {
		this.result = error;
		this.message = message;
	}

	public void processResult() {
		switch (result) {
		case SUCCESS: return;
		case OBJECT_NOT_FOUND:
			throw DBLogger.newObjectNotFoundException(message);
		default:
			throw new UnsupportedOperationException();
		}
	}

	public RESULT result() {
		return result;
	}
	
}
