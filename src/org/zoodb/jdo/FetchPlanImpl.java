package org.zoodb.jdo;

import java.util.Collection;
import java.util.Set;

import javax.jdo.FetchPlan;

import org.zoodb.jdo.stuff.DatabaseLogger;

public class FetchPlanImpl implements FetchPlan {

	@Override
	public FetchPlan addGroup(String fetchGroupName) {
		//TODO
		DatabaseLogger.debugPrint(1, "STUB FecthPlanImpl");
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
		DatabaseLogger.debugPrint(1, "STUB FecthPlanImpl");
		return this;
	}

	@Override
	public Set getGroups() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

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
		DatabaseLogger.debugPrint(1, "STUB FecthPlanImpl");
		return this;
	}

	@Override
	public int getMaxFetchDepth() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return 0;
	}

	@Override
	public FetchPlan setDetachmentRoots(Collection roots) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Collection getDetachmentRoots() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public FetchPlan setDetachmentRootClasses(Class... rootClasses) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

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
