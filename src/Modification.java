
public class Modification {
   private String type;
   private byte[] mod;
   private int i;
   private int j;
   
   public Modification(String t, byte[] d, int a,int b){
	   type =t;
	   mod = d;
	   i = a;
	   j = b;
   }
   public int getFirstBytePosition(){
	   return i;
   }
   public int getSecondBytePosition(){
	   return j;
   }
   public String getType(){
	   return type;
   }
   public byte[] getModification(){
	   return mod;
   }
   public void append(byte[] n){
	   byte[] combined = new byte[mod.length + n.length];

	   System.arraycopy(mod,0,combined,0, mod.length);
	   System.arraycopy(n,0,combined,mod.length,n.length);
	   mod=combined;
   }
   public void insertFront(byte[] n){
	   byte[] combined = new byte[mod.length + n.length];

	   System.arraycopy(n,0,combined,0, n.length);
	   System.arraycopy(mod,0,combined,n.length,mod.length);
	   mod=combined;   
   }
   public void setModification(byte[] n) {
	   byte[] combined = new byte[n.length];
	   System.arraycopy(n,0,combined,0, n.length);
	   mod = n;	
   }
}
