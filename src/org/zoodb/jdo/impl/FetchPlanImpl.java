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
package org.zoodb.jdo.impl;

import java.util.Collection;
import java.util.Set;

import javax.jdo.FetchPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchPlanImpl implements FetchPlan {

	public static final Logger LOGGER = LoggerFactory.getLogger(FetchPlan.class);

	
	@Override
	public FetchPlan addGroup(String fetchGroupName) {
		//TODO
		LOGGER.warn("STUB FecthPlanImpl");
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
		LOGGER.warn("STUB FecthPlanImpl");
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
		LOGGER.warn("STUB FecthPlanImpl");
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
