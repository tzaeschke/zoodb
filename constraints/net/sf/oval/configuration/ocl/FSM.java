package net.sf.oval.configuration.ocl;

public class FSM {
	public enum STATE {INITIAL, PACKAGE, CONTEXT};
	
	private STATE state;
	
	FSM(){
		state = FSM.STATE.INITIAL;
	}
	
	public STATE getState(){
		return state;
	}
	
	public void setState(STATE state){
		this.state = state;
	}
}
