/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.api;

/**
 * Instance events used for event listeners.
 * 
 * @author Tilmann Zaeschke
 *
 */
public enum ZooInstanceEvent {
	PRE_STORE,
	POST_STORE,
	PRE_DIRTY,
	POST_DIRTY,
	LOAD,
	PRE_DELETE,
	POST_DELETE,
	PRE_CLEAR,
	POST_CLEAR,
	CREATE;
}
