
public class Partition {
	   private byte[] content;
	   private long length;
	   
	   public Partition(byte[] c, int l){
		   content = c;
		   length = l;
	   }
	   public byte[] getContent(){
		   return content;
	   }
	   public long getLength(){
		   return length;
	   }
}
