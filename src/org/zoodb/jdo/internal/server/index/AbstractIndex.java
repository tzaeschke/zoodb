package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.server.PageAccessFile;


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
