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


public class ComplexHolder2 extends ComplexHolder1 {
	
	public int i2;
	
	public int getI2() {
        zooActivateRead();
		return i2;
	}

	public void setI2(int i2) {
    	zooActivateWrite();
		this.i2 = i2;
	}

	@Override
	public long ownCheckSum() {
        zooActivateRead();
		return i2 + super.ownCheckSum();
	}

	@Override
	protected void setSpecial(int value) {
    	zooActivateWrite();
		super.setSpecial(value);
		i2 = value;
	}


}
