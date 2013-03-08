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

import org.zoodb.profiler.test.data.CheckSummable;
import org.zoodb.profiler.test.data.JB3;



public class JB4 extends JB3 implements CheckSummable {
    
    public JB4(){
    }
    
    public JB4(int i0, int i1, int i2, int i3, int i4){
        super(i0, i1, i2, i3);
        b4 = i4;
    }
    
    private int b4;
    
    public void setB4(int i){
    	activateWrite("b4");
        b4 = i;
    }
    
    public int getB4(){
    	activateRead("b4");
        return b4;
    }

    public long checkSum() {
    	activateRead("b4");
        return b4;
    }
    
    public void setAll(int i){
        setB0(i);
        setB1(i);
        setB2(i);
        setB3(i);
        setB4(i);
    }
    
    public int getBx(int x){
        if(x == 0){
            return getB0();
        }
        if(x == 1){
            return getB1();
        }
        if(x == 2){
            return getB2();
        }
        if(x == 3){
            return getB3();
        }
        if(x == 4){
            return getB4();
        }
        throw new IllegalArgumentException();
    }

}
