import java.util.LinkedList;

public class UpdateResult {
	private int k_0;
	private int k_1;
	private LinkedList<Partition> B_p;
	
	public UpdateResult(int a, int b, LinkedList<Partition> B){
		k_0 = a;
		k_1 = b;
		B_p = (LinkedList<Partition>) B.clone();
	}
	public int getFirst(){
		return k_0;
	}
	public int getSecond(){
		return k_1;
	}
	public LinkedList<Partition> getPartitions(){
		return B_p;
	}
}
