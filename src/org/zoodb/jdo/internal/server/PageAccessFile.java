package org.zoodb.jdo.internal.server;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public interface PageAccessFile extends SerialInput, SerialOutput {

	void seekPageForRead(int nextPage, boolean autoPaging);

	void seekPageForWrite(int nextPage, boolean autoPaging);

	void seekPage(int i, int j, boolean autoPaging);

	void close();

	void flush();

	int getOffset();

    int getPage();  

	/**
	 * Allocate a new page. 
	 * @param autoPaging Whether auto paging should be used.
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @return ID of the new page.
	 */
	int allocateAndSeek(boolean autoPaging, int previousPageId);

	int statsGetWriteCount();

	void noCheckWrite(long[] array);

	void noCheckRead(long[] array);

	void noCheckRead(int[] array);

	void noCheckWrite(int[] array);

	void noCheckRead(byte[] array);

	void noCheckWrite(byte[] array);

	int getPageSize();

	/**
	 * 
	 * @return A new PageAccessFile that operates on the same file but provides its own buffer. 
	 */
	PageAccessFile split();

	/**
	 * Callback for page overflow (automatic allocation of following page).
	 * @param overflowCallback
	 */
	void setOverflowCallback(PagedObjectAccess overflowCallback);

	void seekPos(long pageAndOffs, boolean autoPaging);

	void releasePage(int pageId);

	void noCheckWriteAsInt(long[] array, int nElements);

	void noCheckReadAsInt(long[] array, int nElements);
}
