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



public class InheritanceHierarchy4 extends InheritanceHierarchy3 implements CheckSummable{
    
    public InheritanceHierarchy4(){
    }
    
    public InheritanceHierarchy4(int i0, int i1, int i2, int i3, int i4){
        super(i0, i1, i2, i3);
        this.i4 = i4;
    }
    
    private int i4;
    
    public void setI4(int i){
    	zooActivateWrite();
        i4 = i;
    }
    
    public int getI4(){
        zooActivateRead();
        return i4;
    }

    public long checkSum() {
        zooActivateRead();
        return i4;
    }
    
    public void setAll(int i){
    	zooActivateWrite();
        setI0(i);
        setI1(i);
        setI2(i);
        setI3(i);
        setI4(i);
    }
    
    public int getIx(int x){
        if(x == 0){
            return getI0();
        }
        if(x == 1){
            return getI1();
        }
        if(x == 2){
            return getI2();
        }
        if(x == 3){
            return getI3();
        }
        if(x == 4){
            return getI4();
        }
        throw new IllegalArgumentException();
    }

}
