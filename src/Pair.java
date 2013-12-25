import java.io.Serializable;


public class Pair<T,B> implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2423070581232886441L;
	private T first;
	private B second;
	
	public Pair(T a, B b){
		first = a;
		second = b;
	}
	
	public T getFirst(){
		return first;
		
	}
	
	public B getSecond(){
		return second;
	}
	
	public void setFirst(T a){
		first = a;
	}
	
	public void setSecond(B b){
		second = b;
	}
}
