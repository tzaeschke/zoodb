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

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * @author Christian Ernst
 */
public class JN1 extends PersistenceCapableImpl {

    public String s0;
    
    public String s1;
    
    public String s2;
    
    public String s3;
    
    public String s4;
    
    public String s5;
    
    public String s6;
    
    public String s7;
    
    public String s8;
    
    public String s9;

    
    public JN1(){
        
    }
    
    
    public static JN1 generate(int index){
        JN1 n1 = new JN1();
        n1.setStrings(index);
        return n1;
    }
    
    private void setStrings(int index){
        zooActivate(this);
        String str = "N" + index + "aabbccddeeffgghhjjKK";
        str = str.substring(0,20);
        s0 = str;
        s1 = str;
        s2 = str;
        s3 = str;
        s4 = str;
        s5 = str;
        s6 = str;
        s7 = str;
        s8 = str;
        s9 = str;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS0() {
        zooActivate(this);
        return s0;
    }

    
    public void setS0(String s0) {
        zooActivate(this);
        this.s0 = s0;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS1() {
        zooActivate(this);
        return s1;
    }

    
    public void setS1(String s1) {
        zooActivate(this);
        this.s1 = s1;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS2() {
        zooActivate(this);
        return s2;
    }

    
    public void setS2(String s2) {
        zooActivate(this);
        this.s2 = s2;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS3() {
        zooActivate(this);
        return s3;
    }

    
    public void setS3(String s3) {
        zooActivate(this);
        this.s3 = s3;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS4() {
        zooActivate(this);
        return s4;
    }

    
    public void setS4(String s4) {
        zooActivate(this);
        this.s4 = s4;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS5() {
        zooActivate(this);
        return s5;
    }

    
    public void setS5(String s5) {
        zooActivate(this);
        this.s5 = s5;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS6() {
        zooActivate(this);
        return s6;
    }

    
    public void setS6(String s6) {
        zooActivate(this);
        this.s6 = s6;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS7() {
        zooActivate(this);
        return s7;
    }

    
    public void setS7(String s7) {
        zooActivate(this);
        this.s7 = s7;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS8() {
        zooActivate(this);
        return s8;
    }

    
    public void setS8(String s8) {
        zooActivate(this);
        this.s8 = s8;
		JDOHelper.makeDirty(this, "");
    }

    
    public String getS9() {
        zooActivate(this);
        return s9;
    }

    
    public void setS9(String s9) {
        zooActivate(this);
        this.s9 = s9;
		JDOHelper.makeDirty(this, "");
    }

    
}
