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
		/*BufferedReader br = new BufferedReader(new FileReader(folder));
		String line;
		LinkedList<Integer> meta = new LinkedList<Integer>();
		try {
			while ((line = br.readLine()) != null) {
				meta.add(Integer.parseInt(line));
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		File D_out = new File(output.getAbsolutePath()+"/"+folder.getName());
		if(D_out.exists())
			D_out.delete();
		FileOutputStream fos = new FileOutputStream(D_out);
		File meta_file = new File(folder.getAbsolutePath()+"/meta");
		ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(meta_file));
		@SuppressWarnings("unchecked")
		LinkedList<Pair<String, Integer>> metafile = (LinkedList<Pair<String, Integer>>) inputStream.readObject();
		inputStream.close();
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





	public static void main(String []args) throws IOException, ClassNotFoundException{
		FileManager fm = new FileManager();
		File f = new File("/Users/Andy/Downloads/short1.txt");
		fm.splitFiles(f);
		File f1 = new File(fm.save.getAbsolutePath()+"/short1.txt");
		fm.mergeFiles(f1);
	}
}
