/* Copyright (C) 2004 - 2006  db4objects Inc.  http://www.db4o.com */

package org.zoodb.test.data;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.JDOHelper;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * @author Christian Ernst
 */
public class JdoListHolder extends PersistenceCapableImpl implements CheckSummable{
    
    public List<Integer> list;
    
    public JdoListHolder(){
        
    }
    
    public static JdoListHolder generate(int index, int elements){
        JdoListHolder lh = new JdoListHolder();
        List<Integer> list = new ArrayList<Integer>(); 
        lh.setList(list);
        for (int i = 0; i < elements; i++) {
            list.add(new Integer(i));
        }
        return lh;
    }


    public long checkSum() {
        zooActivate(this);
        return list.size();
    }

    public List<Integer> getList() {
        zooActivate(this);
        return list;
    }
    
    public void setList(List<Integer> list) {
        zooActivate(this);
        this.list = list;
		JDOHelper.makeDirty(this, "");
    }

}
