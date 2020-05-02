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

import org.zoodb.internal.util.DBLogger;

/**
 * 
 * @author Tilmann Zäschke
 *
 */
public class ServerResponse {

	public enum RESULT {
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
