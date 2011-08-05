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


package org.zoodb.test.data;

import javax.jdo.JDOHelper;

public class ComplexHolder3 extends ComplexHolder2 {
	
	public int i3;
	
	public int getI3() {
        zooActivate(this);
		return i3;
	}

	public void setI3(int i3) {
        zooActivate(this);
		this.i3 = i3;
		JDOHelper.makeDirty(this, "");
	}

	@Override
	public long ownCheckSum() {
        zooActivate(this);
		return i3 + super.ownCheckSum();
	}

	@Override
	protected void setSpecial(int value) {
        zooActivate(this);
		super.setSpecial(value);
		i3 = value;
		JDOHelper.makeDirty(this, "");
	}

}
