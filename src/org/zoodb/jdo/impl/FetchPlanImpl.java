/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.impl;

import java.util.Collection;
import java.util.Set;

import javax.jdo.FetchPlan;

import org.zoodb.internal.util.DBLogger;

public class FetchPlanImpl implements FetchPlan {

	@Override
	public FetchPlan addGroup(String fetchGroupName) {
		//TODO
		DBLogger.debugPrint(1, "STUB FecthPlanImpl");
		return this;
	}

	@Override
	public FetchPlan removeGroup(String fetchGroupName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan clearGroups() {
		//TODO
		DBLogger.debugPrint(1, "STUB FecthPlanImpl");
		return this;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getGroups() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchPlan setGroups(Collection fetchGroupNames) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan setGroups(String... fetchGroupNames) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan setGroup(String fetchGroupName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan setMaxFetchDepth(int fetchDepth) {
		//TODO
		DBLogger.debugPrint(1, "STUB FecthPlanImpl");
		return this;
	}

	@Override
	public int getMaxFetchDepth() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchPlan setDetachmentRoots(Collection roots) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection getDetachmentRoots() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchPlan setDetachmentRootClasses(Class... rootClasses) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class[] getDetachmentRootClasses() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan setFetchSize(int fetchSize) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public int getFetchSize() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	@Override
	public FetchPlan setDetachmentOptions(int options) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public int getDetachmentOptions() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

}
