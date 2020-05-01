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
package org.zoodb.internal.server.index;


public abstract class AbstractPageIterator<E> implements LongLongIndex.LongLongIterator<E> {

	protected final AbstractPagedIndex ind;
	
	private final int modCount;
	private final long txId;
	
	protected AbstractPageIterator(AbstractPagedIndex ind) {
		this.ind = ind;
		this.modCount = ind.getModCount();
		this.txId = ind.getTxId(); 
	}
	
	protected final void releasePage(AbstractIndexPage oldPage) {
		//TODO remove me or use for page locking? Buffer management?
		
		
		//just try it, even if it is not in the list.
//		if (pageClones.remove(oldPage.getOriginal()) == null && oldPage.getOriginal() != null) {
//		    System.out.println("Cloned page not found!");
//		}
	}
	
	protected final AbstractIndexPage findPage(AbstractIndexPage currentPage, short pagePos) {
		return currentPage.readCachedPage(pagePos);
	}

	protected void checkValidity() {
		ind.checkValidity(modCount, txId);
	}
}