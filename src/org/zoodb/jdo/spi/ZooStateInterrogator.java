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
package org.zoodb.jdo.spi;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.StateInterrogation;

import org.zoodb.api.impl.ZooPCImpl;


/**
 * StateInterrogator for JDO.
 * This is only used when the persistent classes implement ZooPCImpl directly instead of 
 * PersistenceCapableImpl. This interrogator is then used when calling e.g. JDOHelper.isDirty().
 * 
 * 
 * @author ztilmann
 *
 */
public class ZooStateInterrogator implements StateInterrogation {

	private boolean checkZPC(Object pc) {
		return pc instanceof ZooPCImpl;
	}
	
	@Override
	public Boolean isPersistent(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooIsPersistent();
	}

	@Override
	public Boolean isTransactional(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooIsTransactional();
	}

	@Override
	public Boolean isDirty(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooIsDirty();
	}

	@Override
	public Boolean isNew(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooIsNew();
	}

	@Override
	public Boolean isDeleted(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooIsDeleted();
	}

	@Override
	public Boolean isDetached(Object pc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		ZooPCImpl zpc = (ZooPCImpl) pc;
		return (PersistenceManager) zpc.jdoZooGetContext().getSession().getExternalSession();
	}

	@Override
	public Object getObjectId(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooGetOid();
	}

	@Override
	public Object getTransactionalObjectId(Object pc) {
		if (!checkZPC(pc)) {
			return null;
		}
		return ((ZooPCImpl)pc).jdoZooGetOid();
	}

	@Override
	public Object getVersion(Object pc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean makeDirty(Object pc, String fieldName) {
		if (!checkZPC(pc)) {
			return false;
		}
		((ZooPCImpl)pc).jdoZooMarkDirty();
		return true;
	}

}
