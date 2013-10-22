package ch.ethz.oserb.configurer;

public class FSM {
	public enum STATE {INITIAL, COLLECTING};
	
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
