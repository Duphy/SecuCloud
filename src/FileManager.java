import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	public FileManager(){
		save = new File("Saves");
		if(!save.exists())
			save.mkdirs();
		output = new File("Output");
		if(!output.exists())
			output.mkdirs();
	}
	void splitFiles(File D) throws IOException{
		long off = 0;
		long f_size = D.length();
		Random r = new Random();
		InputStream ios = new FileInputStream(D);
		File P = new File(save.getAbsolutePath()+"/"+D.getName());
		LinkedList<Pair<String, Integer>> meta = new LinkedList<Pair<String, Integer>>();
		int counter = 0;
		if(!P.exists())
			P.mkdirs();
		else{//if a previous version exists
			Date date = new Date();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
			String path = P.getAbsolutePath();
			P = new File(path+"_"+dateFormat.format(date));
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			FileOutputStream fos;
			File segment = new File(P.getAbsolutePath()+"/"+counter);
			try {
				fos = new FileOutputStream(segment);
				fos.write(buffer);
				fos.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			counter++;
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

	public void bytesUpdate(File D, Modification M) throws FileNotFoundException, IOException, ClassNotFoundException{
		int k_0;
		int k_1;
		int l;
		int i = M.getFirstIndex();
		int c = M.getModification().length;
		LinkedList<Pair<String, Integer>> metafile = extractMeta(D);
		int size = getFileSize(metafile);
		if(i==-1){//insert at beginning of the file
			k_0=-1;
		}
		else if(i==size){//append to the file
			k_0 = metafile.size()-1;
			k_1 = Integer.MAX_VALUE;
		}
		else{
			k_0 = Math.min(size, i);
			l = getPartitionsSize(metafile, 0, k_0)-i;
			if(l>0){//the size before the insert index is larger than insert size
				//M.appendModification(n);

			}
		}

	}
	private int getPartitionsSize(LinkedList<Pair<String, Integer>> metafile,int initial,
			int end) {
		int size = 0;
		for(int i=initial;i<=end;i++){
			size+=metafile.get(i).getSecond();
		}
		return size;
	}
	public LinkedList<Pair<String, Integer>> extractMeta(File folder) throws FileNotFoundException, IOException, ClassNotFoundException{
		File meta_file = new File(folder.getAbsolutePath()+"/meta");
		ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(meta_file));
		@SuppressWarnings("unchecked")
		LinkedList<Pair<String, Integer>> metafile = (LinkedList<Pair<String, Integer>>) inputStream.readObject();
		inputStream.close();
		return metafile;
	}
	public int getFileSize(LinkedList<Pair<String, Integer>> metafile){
		int size = 0;
		for(int i=0;i<metafile.size();i++){
			size+=metafile.get(i).getSecond();
		}
		return size;
	}
	public byte[] getPartitioncontent(LinkedList<Pair<String, Integer>> metafile, int index) throws IOException{
		Pair<String, Integer> target = metafile.get(index);
		File f1=new File(target.getFirst());  
		int len = target.getSecond();
		InputStream binputStream= new FileInputStream(f1);  
		byte buf[]=new byte[len];
		binputStream.read(buf);
		binputStream.close();
		return buf;
	}
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
	public static void main(String []args) throws IOException, ClassNotFoundException{
		FileManager fm = new FileManager();
		File f = new File("/Users/Andy/Downloads/short1.txt");
		//fm.splitFiles(f);
		File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		//fm.mergeFiles(f1);
		LinkedList<Pair<String, Integer>> metafile = fm.extractMeta(f1);
		int first = 0;
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
