package org.zoodb.profiling.model2;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class PublicationSplit extends PersistenceCapableImpl {

	private long downloadCount;
	private long citationCount;
	private long viewCount;
	
	public int getDownloadCount() {
		activateRead("downloadCount");
		return (int) downloadCount;
	}
	public void setDownloadCount(int downloadCount) {
		activateWrite("downloadCount");
		this.downloadCount = downloadCount;
	}
	public int getCitationCount() {
		activateRead("citationCount");
		return (int) citationCount;
	}
	public void setCitationCount(int citationCount) {
		activateWrite("citationCount");
		this.citationCount = citationCount;
	}
	public int getViewCount() {
		activateRead("viewCount");
		return (int) viewCount;
	}
	public void setViewCount(int viewCount) {
		activateWrite("viewCount");
		this.viewCount = viewCount;
	}

}
