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
