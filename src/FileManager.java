import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.LinkedList;
class FileManager {
	private static final int Blksize = 40000;
	private File save;
	private File output;
	private String password = "qwe";
	public FileManager(){
		save = new File("Saves");
		if(!save.exists())
			save.mkdirs();
		output = new File("Output");
		if(!output.exists())
			output.mkdirs();
	}
	public void setPassword(String p){
		password = p;
	}
	void splitFiles(File D) throws IOException, NoSuchAlgorithmException{
		long off = 0;
		long f_size = D.length();
		Random r = new Random();
		InputStream ios = new FileInputStream(D);
		File P = new File(save.getAbsolutePath()+"/"+D.getName());
		LinkedList<Pair<String, Integer>> meta = new LinkedList<Pair<String, Integer>>();
		if(!P.exists())
			P.mkdirs();
		else{//if a previous version exists
			Date date = new Date();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			String path = P.getAbsolutePath();
			P.delete();
			//P = new File(path+"_"+dateFormat.format(date));
			P = new File(save.getAbsolutePath()+"/"+D.getName());
			P.mkdirs();
		}
		while(off < f_size){
			int R = (int) Math.abs(r.nextLong() %Blksize);
			if(off+R<f_size)
				off+=R;
			else{
				R = (int) (f_size-off);
				off = f_size;
			}
			byte []buffer = new byte[Blksize];//initial the block to 0
			try {
				ios.read(buffer,0,R);//read up to R bytes with auto padding
			} catch (IOException e) {
				e.printStackTrace();
			}
			FileOutputStream fos;
			File segment = new File(P.getAbsolutePath()+"/"+hash(buffer).toString());
			try {
				fos = new FileOutputStream(segment);
				fos.write(buffer);
				fos.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}			
			meta.add(new Pair<String, Integer>(segment.getAbsolutePath(),R));
		}
		ios.close();
		//write meta file
		//PrintWriter out = new PrintWriter(new FileWriter(P.getAbsolutePath()+"/meta"));
		ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(P.getAbsolutePath()+"/meta"));
		//for(int i = 0; i<meta.size();i++){
		outputStream.writeObject(meta);
		//}
		outputStream.close();	
		System.out.println("split finish");
	}

	void mergeFiles(File folder) throws IOException, ClassNotFoundException{
		File D_out = new File(output.getAbsolutePath()+"/"+folder.getName());
		if(D_out.exists())
			D_out.delete();
		FileOutputStream fos = new FileOutputStream(D_out);
		LinkedList<Pair<String, Integer>> metafile = extractMeta(folder);
		for (int i = 0; i<metafile.size();i++){
			int len = metafile.get(i).getSecond();  
			File f1=new File(metafile.get(i).getFirst());  
			InputStream binputStream= new FileInputStream(f1);  
			byte buf[]=new byte[len];
			binputStream.read(buf);
			binputStream.close();
			fos.write(buf);  
		}
		fos.close();
		System.out.println("merge finish");
	}

	public static UpdateResult bytesUpdate(File D, Modification M) throws FileNotFoundException, IOException, ClassNotFoundException{
		int k_0 = 0;//before this index, No block is changed
		int k_1 = 0;//after this block, No block is changed
		final int INF = -2;
		int l = 0;//accumulate length
		int i = M.getFirstBytePosition();
		int j = M.getSecondBytePosition();
		LinkedList<Integer> u_p = new LinkedList<Integer>();
		LinkedList<Partition> B_p= new LinkedList<Partition>();
		int c = M.getModification().length;
		boolean flag_10 = false;
		boolean flag_15 = false;
		boolean flag_16 = false;
		boolean flag_27 = false;
		LinkedList<Pair<String, Integer>> metafile = extractMeta(D);
		int size = getFileSize(metafile);
		if(i==-1){//insert at beginning of the file
			k_0=-1;//go to step 10
			flag_10 = true;
		}
		else if(i>=size){//append to the file
			k_0 = metafile.size()-1;
			k_1 = metafile.size();//indicate infinately
			flag_15 = true;
		}//go to step 15
		if(!flag_10&&!flag_15){
			int index = getPartitionIndex(metafile,i);
			k_0 = index;
			l = getPartitionsSize(metafile, 0, k_0)-i;
			if(l>0){//the size before the insert index is larger than insert size. step 8
				int u_k0 = metafile.get(k_0).getSecond();//length
				int len = u_k0-l;
				byte[] content = null;
				if(len>0){
					content = getPartContent(metafile, k_0,0,len);
				}
				else{
					len=0;
				}
				//System.out.println("Before is ");
				//System.out.println(Arrays.toString(content));
				M.insertFront(content);//append the modification to the begin of file
				
				c=c+len;
				k_0=k_0-1;
			}
		}
		if(!flag_15){
			k_1=getPartitionIndex(metafile,j);//step 10
			l=getPartitionsSize(metafile,0,k_1)-j;//get length of the remaining bytes in B_k1
			if(l>0){
				int u_k1 = metafile.get(k_1).getSecond();//length
				int start = u_k1-l+1;
				int end = u_k1;
				//System.out.println("start is "+ start);
				//System.out.println("end is "+ end);
				byte[] content = getPartContent(metafile, k_1,start,end);
				//System.out.println("After is ");
				//System.out.println(Arrays.toString(content));
				M.append(content);//append the modification to the end of file
				c=c+l-1;
			}
			k_1++;//step 14
		}
		//step 15
		int x = 0;
		while(!flag_27){
			if(!flag_16){
				Random r = new Random();
				x = (int) Math.abs(r.nextLong() %Blksize);
			}
			if(x==c){//step 16 
				Partition B = new Partition(M.getModification(),x);
				B_p.add(B);
				flag_27 = true;//go to step27
			}
			else if(x<c){
				c=c-x;
				byte[] temp = new byte[x];
				System.arraycopy(M.getModification(),0,temp,0,x-1);
				Partition B = new Partition(temp,x);
				byte[] left = new byte[c];
				System.arraycopy(M.getModification(),x,left,0,c-1);
				M.setModification(left);
				flag_16 = false;//go to step 15
			}
			else if(x>c){
				if(k_1==metafile.size()){
					Partition B = new Partition(M.getModification(),c);
					B_p.add(B);
					k_1 = metafile.size();
					flag_27 = true;//go to step 27
				}
				else{
					int u_k1 =metafile.get(k_1).getSecond();
					c+=u_k1;
					M.append(getPartition(metafile, k_1));
					k_1++;
					flag_16 = true;//TODO: go to step 16
				}
			}
		}//step 15 while
		return (new UpdateResult(k_0,k_1,B_p));
	}
	public static void top(File D, Modification M) throws FileNotFoundException, IOException, ClassNotFoundException, NoSuchAlgorithmException{
		UpdateResult results = bytesUpdate(D, M);
		//delete affect positions of metafile first
		LinkedList<Pair<String, Integer>> metafile = extractMeta(D);
		int start_point = Math.max(0,results.getFirst());
		int end_point = Math.min(metafile.size(),results.getSecond());
		LinkedList<Pair<String, Integer>> new_meta = new LinkedList<Pair<String, Integer>>();
		for(int j = 0; j<start_point;j++){
			//File delete = new File(metafile.get(j).getFirst());
		    //delete.delete();
			new_meta.add(metafile.get(j));
			//metafile.remove(j);
		}
		for(int i=0;i<results.getPartitions().size();i++){

			byte[] buffer = new byte[Blksize];
			//get modified partition contents and store in the disk
			//System.out.println((int)results.getPartitions().get(i).getLength());
			//System.out.println(Arrays.toString(results.getPartitions().get(i).getContent()));
			byte[] temp = results.getPartitions().get(i).getContent();
			System.arraycopy(temp,0,buffer,0, temp.length);
			FileOutputStream fos;
			File segment = new File(D.getAbsolutePath()+"/"+hash(buffer).toString());
			fos = new FileOutputStream(segment);
			fos.write(buffer);
			fos.close();
			Pair<String, Integer> buf = new Pair<String, Integer>(segment.getAbsolutePath(),
					(int)results.getPartitions().get(i).getLength());
			new_meta.add(start_point, buf);
			start_point++;
		}
		for(int m = end_point; m<metafile.size();m++){
			new_meta.add(metafile.get(m));
		}
		for(int n = start_point; n<end_point;n++){
			File delete = new File(metafile.get(n).getFirst());
		    delete.delete();
		}
		//write meta file
		//PrintWriter out = new PrintWriter(new FileWriter(P.getAbsolutePath()+"/meta"));
		ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(D.getAbsolutePath()+"/meta"));
		//for(int i = 0; i<meta.size();i++){
		outputStream.writeObject(new_meta);
		//}
		outputStream.close();	
	}
	private static int getPartitionIndex(LinkedList<Pair<String, Integer>> metafile, int i) {
		int sum = 0;
		int index = 0;
		while (sum<i){
			Pair<String, Integer> part = metafile.get(index);
			sum += part.getSecond();
			if(sum>=i)
				break;
			else{
				index++;
			}
		}
		return index;
	}
	//get the size of some continuous partitions
	private static int getPartitionsSize(LinkedList<Pair<String, Integer>> metafile,int initial,
			int end) {
		int size = 0;
		for(int i=initial;i<=end;i++){
			size+=metafile.get(i).getSecond();
		}
		return size;
	}
	//get the meta file of a split folder
	public static LinkedList<Pair<String, Integer>> extractMeta(File folder) throws FileNotFoundException, IOException, ClassNotFoundException{
		File meta_file = new File(folder.getAbsolutePath()+"/meta");
		ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(meta_file));
		@SuppressWarnings("unchecked")
		LinkedList<Pair<String, Integer>> metafile = (LinkedList<Pair<String, Integer>>) inputStream.readObject();
		inputStream.close();
		return metafile;
	}
	//get the total size of a split file
	public static int getFileSize(LinkedList<Pair<String, Integer>> metafile){
		int size = 0;
		for(int i=0;i<metafile.size();i++){
			size+=metafile.get(i).getSecond();
		}
		return size;
	}
	//get the partition content of a partition
	public static byte[] getPartition(LinkedList<Pair<String, Integer>> metafile, int index) throws IOException{
		Pair<String, Integer> target = metafile.get(index);
		File f1=new File(target.getFirst());  
		int len = target.getSecond();
		InputStream binputStream= new FileInputStream(f1);  
		byte buf[]=new byte[len];
		binputStream.read(buf);
		binputStream.close();
		return buf;
	}
	public static byte[] getPartContent(LinkedList<Pair<String, Integer>> metafile, int index, int start, int end) throws IOException{
		Pair<String, Integer> target = metafile.get(index);
		File f1=new File(target.getFirst());  
		int len = end - start;
		InputStream binputStream= new FileInputStream(f1);  
		byte buf[]=new byte[len];
		System.out.println("Part Length is "+len);
		binputStream.skip(start);
		binputStream.read(buf);
		binputStream.close();
		return buf;
	}
	//get the content between 2 index of bytes
	public byte[] getContent(LinkedList<Pair<String, Integer>> metafile, int first, int second) throws IOException{
		int sum = 0;
		int i=0;
		long start_position=0;
		long end_position=0;
		boolean flag_start = false;
		boolean flag_first = true;
		int off=0;
		byte[] result = new byte[second-first];
		while (sum<second){
			start_position = 0;
			Pair<String, Integer> target = metafile.get(i);
			i++;
			sum+=target.getSecond();//count bytes
			end_position = Math.min(sum, second);
			if(sum>=first){//reach the first index
				if(flag_first){//if it is first time, start_position may be the middle of a file
							  //rest cases, the start point should be the start of the file
					if(first<target.getSecond())
						start_position = first;
					else
						start_position = first-sum+target.getSecond();//get relative position of the first index
					flag_first=false;
				}
				flag_start = true;//find the first index and start to read byte
				if(sum>=second)
					end_position = second-sum+target.getSecond();//get related position of second
				else
					end_position = target.getSecond();//end of a file
			}
			if(flag_start){
				File f1=new File(target.getFirst());  
				long len = end_position - start_position;
				InputStream binputStream= new FileInputStream(f1);  
				binputStream.skip(start_position);
				binputStream.read(result,off,(int)len);
				off+=len;
				binputStream.close();
			}
			
		}//while
		return result;
	}
	public static byte[] hash(byte[] content) throws NoSuchAlgorithmException {
	    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");        
	    byte[] passHash = sha256.digest(content);
	    return passHash;
	}
	public static void main(String []args) throws FileNotFoundException, NoSuchAlgorithmException, IOException, ClassNotFoundException{
		FileManager fm = new FileManager();
		File f = new File("/Users/Andy/Downloads/short1.txt");
		System.out.println(f.length());
		fm.splitFiles(f);
		File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		byte[] modi = new byte[2];
		LinkedList<Pair<String, Integer>> metafile = fm.extractMeta(f1);
		System.out.println("Length is "+ fm.getFileSize(metafile));
		int b_index = -2;
		int f_index = b_index+1;
		byte[]buf1 = fm.getContent(metafile,0, 10);
		System.out.println(Arrays.toString(buf1));
		Modification M = new Modification("Delete", modi, b_index,f_index); 
		top(f1,M);
		metafile = fm.extractMeta(f1);
		System.out.println("Length is "+ fm.getFileSize(metafile));
		byte[]buf2 = fm.getContent(metafile,0, 10);
		System.out.println(Arrays.toString(buf2));
		System.out.println("finish");
	}
	public static void test_get_content_and_compare(String []args) throws IOException, ClassNotFoundException{
		FileManager fm = new FileManager();
		File f = new File("/Users/Andy/Downloads/short1.txt");
		//File f = new File("/Users/Andy/Downloads/short.nds");
		try {
			fm.splitFiles(f);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		//fm.mergeFiles(f1);
		LinkedList<Pair<String, Integer>> metafile = fm.extractMeta(f1);
		int first = 18049;
		int second = 18151;
		//size of 0 = 18049;
		//byte[] buf1 = fm.getContent(metafile, first, second);
		byte[]buf1 = fm.getPartContent(metafile,1,first-18049,second-18049);
		InputStream binputStream= new FileInputStream(f);  
		binputStream.skip(first);
		byte[] buf2 = new byte[second-first];
		binputStream.read(buf2,0,second -first);
		binputStream.close();
		if (Arrays.equals(buf1, buf2))
			System.out.println("equal");
		else
			System.out.println("not equal");
		

	}
	public void test_get_content() throws IOException, ClassNotFoundException{
		FileManager fm = new FileManager();
		File f = new File("/Users/Andy/Downloads/short1.txt");
		//fm.splitFiles(f);
		File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		//fm.mergeFiles(f1);
		LinkedList<Pair<String, Integer>> metafile = fm.extractMeta(f1);
		int first = 1;
		int second = 10;
		byte[] buf1 = fm.getContent(metafile, first, second);
		InputStream binputStream= new FileInputStream(f);  
		binputStream.skip(first);
		byte[] buf2 = new byte[second-first];
		binputStream.read(buf2,0,second -first);
		binputStream.close();
		if (Arrays.equals(buf1, buf2))
			System.out.println("equal");
		else
			System.out.println("not equal");
		

	}

}
