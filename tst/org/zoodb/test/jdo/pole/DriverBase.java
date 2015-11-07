/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.zoodb.test.jdo.pole;


/**
 * an implementation of a circuit for a team
 */
public abstract class DriverBase 
{
    
    private long _checkSum;

	private int _bulkId;
	
	private int _testId;

	private int _objectCount;
	
	private int _commitInterval;
	
	/**
	 * take a seat in a car.
	 */
	public void configure() {
        _checkSum = 0;
    }

	/**
     * Called after the lap so that the driver can clean up any files it
     * created and close any resources it opened. 
     */
	public abstract void closeDatabase();
    
    
	public void addToCheckSum(CheckSummable checkSummable){
		addToCheckSum(checkSummable.checkSum());
	}
    
    /**
     * Collecting a checksum to make sure every team does a complete job  
     */
    public synchronized void addToCheckSum(long l){
        _checkSum += l;
    }
    
	public long checkSum(){
        return _checkSum; 
    }
    
    public DriverBase clone(){
        try{
            return (DriverBase) super.clone();
        }catch(CloneNotSupportedException e){
            e.printStackTrace();
        }
        return null;
    }
    
	public void circuitCompleted() {
		// This method can be overridden to clean up state.
	}
	
	public boolean supportsConcurrency() {
		return true;
	}

	public void copyStateFrom(DriverBase masterDriver) {
		// default: do nothing
		// Implement for concurrency to copy the state
	}

	public void bulkId(int id) {
		_bulkId = id;
	}
	
	protected void initializeTestId(int count, int commitInterval) {
		_objectCount = count;
		_testId = _bulkId * count;
		_commitInterval = commitInterval;
	}
	
	protected void initializeTestIdD(int count, int commitInterval) {
		_objectCount = count;
		_testId = _bulkId * count;
		_commitInterval = commitInterval;
	}

	
	protected int nextTestId(){
		_objectCount--;
		if(_objectCount < 0) {
			outOfObjectCount();
		}
		return ++_testId;
	}

	private void outOfObjectCount() {
		throw new IllegalStateException(" Out of _objectCount. Did you call initializeTestId ?");
	}
	
//	protected int selects(){
//		return setup().getSelectCount();
//	}
//	
//	protected int objects() {
//		return setup().getObjectCount();
//	}
//	
//	protected int updates() {
//		return setup().getUpdateCount();
//	}
//	
//	protected int depth(){
//		return setup().getDepth();
//	}
//	
//	protected int reuse() {
//		return setup().getReuse();
//	}
//	
//	protected int writes(){
//		return setup().getWrites();
//	}
//	
//	protected int deletes(){
//		return setup().getDeletes();
//	}
	
	protected boolean doCommit(){
		if(_objectCount == 0){
			return true;
		}
		if(_objectCount < 0){
			outOfObjectCount();
		}
		return (_objectCount % _commitInterval) == 0;
	}

	protected boolean hasMoreTestIds() {
		return _objectCount > 0;
	}
	
	public void prepareDatabase(){
		// virtual
	}

}
