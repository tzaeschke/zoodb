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


package org.zoodb.profiler.test.data;

import org.zoodb.test.data.CheckSummable;


public class IndexedObject implements CheckSummable{
	
	private static final int STRING_LENGTH = 20;
	
	private static final String UNDERSCORES = underscores();
	
	public int _int;
	
	public String _string;
	
	public IndexedObject(){
		
	}
	
	public IndexedObject(int int_, String str){
		_int = int_;
		_string = str;
	}
	
	public IndexedObject(int int_){
		this(int_, queryString(int_));
	}

	@Override
	public long checkSum() {
		return _string.length();
	}
	
	public static String queryString(int int_){
		StringBuffer sb = new StringBuffer();
		sb.append("str");
		sb.append(int_);
		sb.append(UNDERSCORES);
		return sb.substring(0, STRING_LENGTH);
	}
	
	private static String underscores() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < STRING_LENGTH; i++) {
			sb.append("_");
		}
		return sb.toString();
	}

	public void updateString() {
		_string = _string.toUpperCase();
	}
	
	@Override
	public String toString() {
		return "IndexedObject _int:" + _int + " _string:" + _string;
	}
	
}
