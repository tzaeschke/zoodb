package org.zoodb.profiling.model2;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class PublicationSplit extends PersistenceCapableImpl {

	private int downloadCount;
	private int citationCount;
	
	public int getDownloadCount() {
		activateRead("downloadCount");
		return downloadCount;
	}
	public void setDownloadCount(int downloadCount) {
		activateWrite("downloadCount");
		this.downloadCount = downloadCount;
	}
	public int getCitationCount() {
		activateRead("citationCount");
		return citationCount;
	}
	public void setCitationCount(int citationCount) {
		activateWrite("citationCount");
		this.citationCount = citationCount;
	}

}
