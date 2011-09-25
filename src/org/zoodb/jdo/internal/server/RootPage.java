package org.zoodb.jdo.internal.server;

final class RootPage {

	private long txId = 1;
	
	private int userPage;
	private int oidPage;
	private int schemaPage; 
	private int indexPage;
	private int freeSpaceIndexPage;
	boolean isDirty(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage) {
		if (this.userPage != userPage || 
				this.oidPage != oidPage || 
				this.schemaPage != schemaPage ||
				this.indexPage != indexPage ||
				this.freeSpaceIndexPage != freeSpaceIndexPage) {
			return true;
		}
		return false;
	}
	
	void set(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage) {
		this.userPage = userPage;
		this.oidPage = oidPage;
		this.schemaPage = schemaPage;
		this.indexPage = indexPage;
		this.freeSpaceIndexPage = freeSpaceIndexPage;
	}

	void setTxId(long txId) {
		this.txId = txId; 
	}

	void incTxId() {
		txId++;
	}

	long getTxId() {
		return txId;
	}

	@Deprecated
	int getUserPage() {
		return userPage; 
	}

	/**
	 * 
	 * @return Index page.
	 * @deprecated This should probably be removed. Are we gonna use this at some point?
	 */
	int getIndexPage() {
		return indexPage;
	}
}
