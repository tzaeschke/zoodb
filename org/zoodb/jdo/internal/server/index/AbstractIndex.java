package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.server.PageAccessFile;


abstract class AbstractIndex {

	protected final PageAccessFile _raf;
	private boolean _isDirty;
	
	public AbstractIndex(PageAccessFile raf, boolean isNew) {
		_raf = raf;
		_isDirty = isNew;
	}
	
	protected final boolean isDirty() {
		return _isDirty;
	}
	
	protected final void markDirty() {
		_isDirty = true;
	}
	
	protected final void markClean() {
		_isDirty = false;
	}
}
