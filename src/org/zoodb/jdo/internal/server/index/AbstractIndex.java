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
package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.server.PageAccessFile;


/**
 * Abstract base class for indices.
 * 
 * @author Tilmann Zäschke
 */
abstract class AbstractIndex {

	protected final PageAccessFile _raf;
	private boolean _isDirty;
	private final boolean _isUnique; 
	
	public AbstractIndex(PageAccessFile raf, boolean isNew, boolean isUnique) {
		_raf = raf;
		_isDirty = isNew;
		_isUnique = isUnique;
	}
	
    protected final boolean isDirty() {
        return _isDirty;
    }
    
    protected final boolean isUnique() {
        return _isUnique;
    }
    
	protected final void markDirty() {
		_isDirty = true;
	}
	
	protected final void markClean() {
		_isDirty = false;
	}
}
