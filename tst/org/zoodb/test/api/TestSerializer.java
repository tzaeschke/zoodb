/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.test.api;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Vector;

import javax.jdo.JDOHelper;
import javax.jdo.spi.PersistenceCapable;

import org.junit.Assert;

import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.jdo.api.DBHashMap;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * Test class to verify correct serialization and de-serialization.
 * 
 * @author Tilmann Zaeschke
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestSerializer extends PersistenceCapableImpl {

	private enum ENUM {
		X, Y;
	}
	
    private transient int TRANS = 23;
    private static int STATIC = 11;
    private transient static int TRANS_STATIC = 48;
    
    private static final boolean B1 = true;
    private static final boolean B2 = false;
    private static final boolean[] bOA = new boolean[]{false, true};
    private static final boolean[] bOAN = null;
    private static final boolean[][] bOANN = new boolean[0][2];
    private static final Boolean BON = null;
    private static final Boolean[] BOA = new Boolean[]{false, true};
    private static final Boolean[] BOAN = null;
    private static final Boolean[][] BOANN = new Boolean[0][2];
    private static final byte B = 123;
    private static final byte[] bA = new byte[]{2, -8, 6};
    private static final byte[][] bAA = new byte[][]{{2, -8, 6}, {-8, 6}};
    private static final Byte BN = null;
    private static final Byte[] BA = new Byte[]{2, -8, 6};
    private static final char C = 'g';
    private static final char[] cA = new char[]{'f', 'h'};
    private static final Character[] CA = new Character[]{'f', 'h'};
    private static final double D = 3.564;
    private static final double[] dA = new double[]{4.2};
    private static final Double[] DA = new Double[]{4.2};
    private static final float F = -5.3F;
    private static final float[] fA = new float[]{1.2F, -4.7F, 8.888F};
    private static final Float[] FA = new Float[]{1.2F, -4.7F, 8.888F};
    private static final int I = 32426432;
    private static final int[] iA = new int[]{13, -132134, 4334652};
    private static final Integer[] IA = new Integer[]{13, -132134, 4334652};
    private static final long L = 3487203425354334L;
    private static final long[] lA = new long[]{41L, -34242345378625121L, 0L};
    private static final Long[] LA = new Long[]{41L, -34242345378625121L, 0L};
    private static final short S = -212;
    private static final short[] sA = new short[]{};
    private static final Short[] SA = new Short[]{};
    
    private static Object O = new TestSuper();
    private static final Object ON = null;
    private static final Object[] OA = new Object[]{};
    private static final Object[] OAN =  null;
    private static TestSuper T = new TestSuper(3, 4, null);
    private static TestSuper TN = null;
    private static TestSuper[] TA = new TestSuper[] {T, null, T};
    private static TestSuper[] TAN = null;
    private static final String St1 = "";
    private static final String St2 = 
        "ehs !@#$%^&*()_+}{dkjf\\awsdkhs\0hkdhah6`'\"|,./<>?shjkl\ndfsa";
    private static final String St3 = null;
    private static final String[] StA = new String []{"ads", "qe"};
    private static final String[][] StAA = 
        new String [][]{{"ads", "qe"}, {"4", null}};
    private static final String[][][] StAAANormal = 
        new String [][][]{
        {{"ads", "qe"}, {"\\", "\\"}, {"ads", "qe"}},
        {{"ads", "qe"}, {"", ""}, {"\\", "qe"}}
        };
    private static final String[][][] StAAAJagged = 
        new String [][][]{
        {{"ads", "qe"}, null, {"ads", "qe"}, {}}, //~4
        {{"ads", "qe"}, {"", ""}, {null, "qe"}},  //3
        {{"ads", null}, {"", ""}, {null, "qe"}, {"a", "b"}, {"a"}}  //5
        };
    private static final File OF = new File(".");

	private final DBArrayList V = new DBArrayList<TestSerializer>();
//    private final DBLargeVector LV = new DBLargeVector(this);
    private final DBHashMap H = new DBHashMap<String, TestSerializer>();
    private final DBArrayList VN = null;
//    private final DBLargeVector LVN = null;
    private final DBHashMap HN = null;
    private final DBHashMap HV = new DBHashMap<Long, PersistenceCapable>();
    private final DBHashMap HO = new DBHashMap<Long, PersistenceCapable>();
    private final DBArrayList VV = new DBArrayList<PersistenceCapable>();
    private final DBArrayList VO = new DBArrayList<PersistenceCapable>();
    
    private static final LinkedList CLL = new LinkedList();
    private static final ArrayList CAL = new ArrayList();
    private static final HashMap CHM = new HashMap();
    private static final HashSet CHS = new HashSet();
    private static final Vector CLV = new Vector();
    private static final Hashtable CLH = new Hashtable();
    
    
    private transient int _trans;
    private static int _static;
    private transient static int _transStatic;
    
    private boolean _bo1;
    private boolean _bo2;
    private boolean[] _boA;
    private boolean[] _boAN;
    private boolean[][] _boANN;
    private byte _b;
    private byte[] _bA;
    private byte[][] _bAA;
    private char _c;
    private char[] _cA;
    private double _d;
    private double[] _dA;
    private float _f;
    private float[] _fA;
    private int _i;
    private int[] _iA;
    private long _l;
    private long[] _lA;
    private short _s;
    private short[] _sA;
    private ENUM _enum;
    private ENUM _enumNull;
    
    private Boolean _Bo1;
    private Boolean _Bo2;
    private Boolean _BoN;
    private Boolean[] _BoA;
    private Boolean[] _BoAN;
    private Boolean[][] _BoANN;
    private Byte _B;
    private Byte _BN;
    private Byte[] _BA;
    private Character _C;
    private Character[] _CA;
    private Double _D;
    private Double[] _DA;
    private Float _F;
    private Float[] _FA;
    private Integer _I;
    private Integer[] _IA;
    private Long _L;
    private Long[] _LA;
    private Short _S;
    private Short[] _SA;
    
    private Object _O;
    private Object _ON;
    private Object[] _OA;
    private Object[] _OAN;
    private File _OF;
    private TestSuper _T;
    private TestSuper _TN;
    private TestSuper[] _TA;
    private TestSuper[] _TAN;
    private String _St1;
    private String _St2;
    private String _St3;
    private String[] _StA;
    private String[][] _StAA;
    private String[][][] _StAAANormal;
    private String[][][] _StAAAJagged;
    
    private DBArrayList _V ;
    private DBLargeVector _LV;
    private DBHashMap _H;
    private DBArrayList _VN;
    private DBLargeVector _LVN;
    private DBHashMap _HN;
    private DBHashMap _HV;
    private DBHashMap _HO;
    private DBArrayList _VV;
    private DBArrayList _VO;
    
    private LinkedList _CLL;
    private ArrayList _CAL;
    private HashMap _CHM;
    private HashSet _CHS;
    private Vector _CLV;
    private Hashtable _CLH;

	public TestSerializer() {
        V.add(this);
        V.add(T);
//        LV.add(this);
//        LV.add(T);
//        LV.add(null);
        HV.put(1, new DBHashMap());
        HV.put(-1, new DBArrayList());
        HO.put(1, "abc");
        HO.put(2, Long.valueOf(123));
//        HO.put(3, new File("abc"));
        HO.put(4, new DBHashMap());
//        HO.put(new File("abc"), 3);
//        HO.put(6, new File("abc6"));
        HO.put(7, new DBHashMap());
//        HO.put(new File("abc7"), 7);
        HO.put(new DBHashMap(), 4);
        VV.add(new DBHashMap());
        VV.add(new DBArrayList());
        VO.add("abcd");
        VO.add(Long.valueOf(1234));
        VO.add(new File("abcd"));//TODO fdfds
        VO.add(new DBArrayList());
    }
     
    static {
        CLL.add("$$");
        CLL.add(false);
        CLL.add(new LinkedList());
        
        CAL.add(3452);
        CAL.add(Boolean.FALSE);
        CAL.add(new LinkedList());
        
        CHM.put(3452, "4few");
        CHM.put(Boolean.FALSE, 232);
        CHM.put(new LinkedList(), new ArrayList());
        
        CHS.add(3452);
        CHS.add(3452);
        CHS.add(Boolean.FALSE);
        CHS.add(new LinkedList());
        CHS.add(3452);
        CHS.add(new LinkedList());
        
        CLV.add("$$");
        CLV.add(false);
        CLV.add(new LinkedList());
        
        CLH.put(3452, "4few");
        CLH.put(Boolean.FALSE, 232);
        CLH.put(new LinkedList(), new ArrayList());
    }
    
    /**
     * 
     */
    public void init() {
    	zooActivateWrite();
        _trans = TRANS;
        _static = STATIC;
        _transStatic = TRANS_STATIC;
        
        _bo1 = B1;
        _bo2 = B2;
        _boA = bOA;
        _boAN = bOAN;
        _boANN = bOANN;
        _b = B;
        _bA = bA;
        _bAA = bAA;
        _c = C;
        _cA = cA;
        _d = D;
        _dA = dA;
        _f = F;
        _fA = fA;
        _i = I;
        _iA = iA;
        _l = L;
        _lA = lA;
        _s = S;
        _sA = sA;
        _enum = ENUM.Y;
        _enumNull = null;
        
        _Bo1 = B1;
        _Bo2 = B2;
        _BoN = BON;
        _BoA = BOA;
        _BoAN = BOAN;
        _BoANN = BOANN;
        _B = B;
        _BN = BN;
        _BA = BA;
        _C = C;
        _CA = CA;
        _D = D;
        _DA = DA;
        _F = F;
        _FA = FA;
        _I = I;
        _IA = IA;
        _L = L;
        _LA = LA;
        _S = S;
        _SA = SA;
        
        _O = O;
        _ON = ON;
        _OA = OA;
        _OAN = OAN;
        _OF = OF;
        _T = T;
        _TN = TN;
        _TA = TA;
        _TAN = TAN;
        _St1 = St1;
        _St2 = St2;
        _St3 = St3;
        _StA = StA;
        _StAA = StAA;
        _StAAANormal = StAAANormal;
        _StAAAJagged = StAAAJagged;
        
        _V = V;
//        _LV = LV;
        _H = H;
        _VN = VN;
//        _LVN = LVN;
        _HN = HN;
        _HV = HV;
        _HO = HO;
        _VV = VV;
        _VO = VO;
        
        _CLL = CLL;
        _CAL = CAL;
        _CHM = CHM;
        _CHS = CHS;
        _CLV = CLV;
        _CLH = CLH;
    }
    
    /**
     * 
     */
    public static void resetStatic() {
        _static = 0;
        _transStatic = 0;

        //reinit persistent classes to avoid session mismatch
        O = new TestSuper();
        T = new TestSuper(3, 4, null);
        TN = null;
        TA = new TestSuper[] {T, null, T};
        TAN = null;
}
    
    /**
     * @param before
     */
    public void check(boolean before) {
    	zooActivateRead();
        if (before) {
            Assert.assertEquals(_trans, TRANS);
            Assert.assertEquals(_static, STATIC);
            Assert.assertEquals(_transStatic, TRANS_STATIC);
        } else {
            Assert.assertEquals(_trans, 0);
            Assert.assertEquals(_static, 0);
            Assert.assertEquals(_transStatic, 0);
        }
        
        Assert.assertEquals(_bo1, B1);
        Assert.assertEquals(_bo2, B2);
        Assert.assertTrue(Arrays.equals(_boA, bOA));
        Assert.assertTrue(Arrays.equals(_boAN, bOAN));
        Assert.assertTrue(Arrays.equals(_boANN, bOANN));
        Assert.assertEquals(_b, B);
        Assert.assertTrue(Arrays.equals(_bA, bA));
        Assert.assertTrue(Arrays.deepEquals(_bAA, bAA));
        Assert.assertEquals(_c, C);
        Assert.assertTrue(Arrays.equals(_cA, cA));
        Assert.assertEquals((Double)_d, (Double)D);
        Assert.assertTrue(Arrays.equals(_dA, dA));
        Assert.assertEquals((Float)_f, (Float)F);
        Assert.assertTrue(Arrays.equals(_fA, fA));
        Assert.assertEquals(_i, I);
        Assert.assertTrue(Arrays.equals(_iA, iA));
        Assert.assertEquals(_l, L);
        Assert.assertTrue(Arrays.equals(_lA, lA));
        Assert.assertEquals(_s, S);
        //Versant stores empty arrays ({}) as null.
        // Assert.assertNull(_sA);
        Assert.assertTrue(Arrays.equals(_sA, sA));
        Assert.assertEquals(_enum, ENUM.Y);
        Assert.assertEquals(_enumNull, null);
        
        Assert.assertEquals((boolean)_Bo1, B1);
        Assert.assertEquals((boolean)_Bo2, B2);
        //Versant maps 'null' to 'false'
        //Assert.assertFalse(_BoN);
        Assert.assertEquals(_BoN, BON);
        Assert.assertEquals(Arrays.deepToString(_BoA), Arrays.deepToString(BOA));
        Assert.assertEquals(Arrays.deepToString(_BoAN), Arrays.deepToString(BOAN));
        Assert.assertEquals(Arrays.deepToString(_BoANN), Arrays.deepToString(BOANN));
        Assert.assertEquals((byte)_B, B);
        //Versant maps 'null' to '0'
        //Assert.assertEquals(_BN, Byte.valueOf((byte)0));
        Assert.assertEquals(_BN, BN);
        Assert.assertEquals(Arrays.deepToString(_BA), Arrays.deepToString(BA));
        Assert.assertEquals((char)_C, C);
        Assert.assertEquals(Arrays.deepToString(_CA), Arrays.deepToString(CA));
        Assert.assertEquals(_D, (Double)D);
        Assert.assertEquals(Arrays.deepToString(_DA), Arrays.deepToString(DA));
        Assert.assertEquals(_F, (Float)F);
        Assert.assertEquals(Arrays.deepToString(_FA), Arrays.deepToString(FA));
        Assert.assertEquals((int)_I, I);
        Assert.assertEquals(Arrays.deepToString(_IA), Arrays.deepToString(IA));
        Assert.assertEquals((long)_L, L);
        Assert.assertEquals(Arrays.deepToString(_LA), Arrays.deepToString(LA));
        Assert.assertEquals((short)_S, S);
        //Versant...
        //Assert.assertNull(_SA);
        Assert.assertEquals(Arrays.deepToString(_SA), Arrays.deepToString(SA));
        
        //Equals doesn't work for arbitrary objects as it compares only
        //object identity.
        Assert.assertNotNull(_O);
        Assert.assertTrue("Expected 'true' but got 'false': O=" + O + 
                " _O=" + _O, O.toString().equals(_O.toString()));
        Assert.assertEquals(_ON, ON);
        Assert.assertNull(_ON);
        Assert.assertEquals(Arrays.deepToString(_OA), Arrays.deepToString(OA));
        Assert.assertNotNull(_OA);
        Assert.assertEquals(_OAN, OAN);
        Assert.assertEquals(_OF, OF);
        Assert.assertEquals(_T, T);
        Assert.assertNotNull(_T);
        Assert.assertEquals(3, T.getTime());
        Assert.assertEquals(4, T.getId());
        Assert.assertEquals(_TN, TN);
        Assert.assertNull(_TN);
        Assert.assertEquals(Arrays.deepToString(_TA), Arrays.deepToString(TA));
        Assert.assertEquals(_TAN, TAN);
        Assert.assertEquals(_St1, St1);
        Assert.assertEquals(_St2, St2);
        Assert.assertEquals(_St3, St3);
        Assert.assertEquals(Arrays.deepToString(_StA), Arrays.deepToString(StA));
        Assert.assertEquals(Arrays.deepToString(_StAA), Arrays.deepToString(StAA));
        Assert.assertEquals(Arrays.deepToString(_StAAANormal), 
                Arrays.deepToString(StAAANormal));
        Assert.assertEquals(Arrays.deepToString(_StAAAJagged), 
                Arrays.deepToString(StAAAJagged));
        
        Assert.assertEquals(_V, V);
//        t.assertEquals(_LV, LV);
        Assert.assertEquals(_H, H);
        Assert.assertEquals(_VN, VN);
//        t.assertEquals(_LVN, LVN);
        Assert.assertEquals(_HN, HN);
        Assert.assertEquals(_HV, HV);
        Assert.assertEquals(_HO, HO);
        for (Object key: HO.keySet()) {
            Assert.assertEquals(_HO.get(key), HO.get(key));
        }
        Assert.assertEquals(_VV, VV);
        Assert.assertEquals(_VO, VO);
        
        Assert.assertEquals(_CLL, CLL);
        Assert.assertEquals(_CAL, CAL);
        Assert.assertEquals(_CHM, CHM);
        Assert.assertEquals(_CHS, CHS);
        Assert.assertEquals(_CLV, CLV);
        Assert.assertEquals(_CLH, CLH);
    }

    /**
     * 
     */
    public void markDirtyTS() {
        JDOHelper.makeDirty(this, null);
        JDOHelper.makeDirty(_V, null);
//        JDOHelper.makeDirty(_LV, null);
        JDOHelper.makeDirty(_H, null);
        JDOHelper.makeDirty(_VV, null);
        JDOHelper.makeDirty(_HV, null);
    }

    /**
     * Does some modifications.
     */
    public void modify() {
    	zooActivateWrite();
        _b = 0;
        _B = 0;
        _bA = null;
        //...
    }
    
	@Override
	public int hashCode() {
		return (int) ((jdoZooGetOid()*10000) + _B + _C + _D + _F + _I + _L + _S);  
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof TestSerializer)) {
			return false;
		}
		TestSerializer o = (TestSerializer) obj;
		if (jdoZooGetOid() != o.jdoZooGetOid()) {
			return false;
		}
		//TODO other attributes?
		return _B==o._B && _C==o._C && _D==o._D && _F==o._F && _I==o._I && _L==o._L && _S==o._S &&
				_b==o._b && _c==o._c && _d==o._d && _f==o._f && _i==o._i && _l==o._l && _s==o._s;
	}
}
