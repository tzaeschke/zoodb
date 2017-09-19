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
package org.zoodb.jdo.spi;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.StateInterrogation;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooHandleImpl;


/**
 * StateInterrogator for JDO.
 * This is only used when the persistent classes implement ZooPC directly instead of 
 * PersistenceCapableImpl. This interrogator is then used when calling e.g. JDOHelper.isDirty().
 * 
 * 
 * @author Tilmann Zaschke
 *
 */
public class ZooStateInterrogator implements StateInterrogation {

	private ZooPC checkZPC(Object pc) {
		if (pc instanceof ZooPC) {
			return (ZooPC) pc;
		}
		if (pc instanceof ZooHandleImpl) {
			return ((ZooHandleImpl)pc).getGenericObject();
		}
		return null;
	}
	
	@Override
	public Boolean isPersistent(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooIsPersistent();
	}

	@Override
	public Boolean isTransactional(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooIsTransactional();
	}

	@Override
	public Boolean isDirty(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooIsDirty();
	}

	@Override
	public Boolean isNew(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooIsNew();
	}

	@Override
	public Boolean isDeleted(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooIsDeleted();
	}

	@Override
	public Boolean isDetached(Object pc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager(Object pc) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return (PersistenceManager) zpc.jdoZooGetContext().getSession().getExternalSession();
	}

	@Override
	public Object getObjectId(Object pc) {
		//Especially for when returning Handles from failed optimistic commit()
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooGetOid();
	}

	@Override
	public Object getTransactionalObjectId(Object pc) {
		//Especially for when returning Handles from failed optimistic commit()
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return null;
		}
		return zpc.jdoZooGetOid();
	}

	@Override
	public Object getVersion(Object pc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean makeDirty(Object pc, String fieldName) {
		ZooPC zpc = checkZPC(pc);
		if (zpc == null) {
			return false;
		}
		zpc.jdoZooMarkDirty();
		return true;
	}

}
