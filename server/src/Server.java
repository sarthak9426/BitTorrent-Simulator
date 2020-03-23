import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import chunkification.src.FileChunkObject;

public class Server {

	private int serverPort;
	static Map<Integer, String> clientMap;
	static Map<Integer, ArrayList<Integer>> clientChunkMap;
	int clientCount = 0;
	static int chunkCount = 0;
	private static final String dir = System.getProperty("user.dir");
	
	ServerSocket receiveSocket;
	Socket connectSocket;
	
	public static void main(String[] args) {
		Server s = new Server();
		try {
			s.divideInputFile();
			File[] chunks  = s.readConfigFile();
			
			if(chunkCount>0)
			{
				System.out.println("Waiting for Connection ...");				
				s.connectToClients(chunks);

			}
			else
			{
				System.out.println("There are no file chunks available");
			}
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void divideInputFile() {
		try {

			System.out.println("Enter the filename:");
			Scanner sc = new Scanner(System.in);
			String fname = sc.nextLine();

			File inputFile = new File(dir+fname);
			Long fileSize = inputFile.length();
			if (fileSize == 0) {
				System.out.println("Please check the file name or enter a valid file");
				sc.close();
				return;
			}
			System.out.println("Input file size is :" + fileSize);

			String newdir = dir + "/chunks/";

			File outFolder = new File(newdir);
			if (outFolder.mkdirs())
				System.out.println("Creating a folder to keep chunks");
			else
				System.out.println("Folder already present");

			byte[] chunk = new byte[102400];
			FileInputStream in = new FileInputStream(inputFile);
			BufferedInputStream inStream = new BufferedInputStream(in);
			int index = 1;
			int bytesRead;
			

			while ((bytesRead = inStream.read(chunk)) > 0) {

				FileOutputStream fileOutStream = new FileOutputStream(
						new File(newdir, inputFile.getName() + "_" + String.format("%04d", index)));
				BufferedOutputStream outStream = new BufferedOutputStream(fileOutStream);
				outStream.write(chunk, 0, bytesRead);
				outStream.close();
				index++;
			}
			inStream.close();
			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public File[] readConfigFile() throws FileNotFoundException {
		String in = null;
		int count = 0;
		BufferedReader br = new BufferedReader(new FileReader(dir + "/configuration.txt"));
		
		String chunksLoc = dir + "/chunks/";
		File[] chunks = new File(chunksLoc).listFiles();
		
		clientMap = new LinkedHashMap<Integer, String>();
		try {
			while ((in = br.readLine()) != null) {
				if (count == 0) {
					String[] tokens = in.split(" ");
					serverPort = Integer.parseInt(tokens[1]);
					count = count + 1;
				} else {
					clientMap.put(clientCount+1, in);
					clientCount++;
				}
			}
			

			chunkCount = chunks.length;
			
			clientChunkMap = new LinkedHashMap<Integer, ArrayList<Integer>>();
			
			for(int i =1 ;i<= clientCount ; i++)
			{
				ArrayList<Integer> arr = new ArrayList<Integer>();
				
				for(int j=i; j<=chunkCount ; j+=clientCount)
				{
					arr.add(j);
				}
				
				clientChunkMap.put(i, arr);
				
			}
			
			br.close();
			
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		return chunks;
		
	}
	
	
	public void connectToClients(File[] chunks) {
		
		try {
					
			int clientNo = 0;
			receiveSocket = new ServerSocket(serverPort);
			while (true) {
				clientNo++;
				if (clientNo <= clientMap.size()) {
					connectSocket = receiveSocket.accept();
					System.out.println("Connection recieved from localhost");
					new ServerThread(connectSocket, chunks, clientChunkMap.get(clientNo), clientMap.get(clientNo)).start();

				} else {
					System.out.println("Server at capacity");
					break;
				}
			}

			
		}
		catch( Exception e ) {
			e.printStackTrace();
		}

	}
}

class ServerThread extends Thread {

	private Socket socket;
	File[] files;
	ObjectOutputStream outStream;
	ArrayList<Integer> chunkList;
	String configClient;

	ServerThread(Socket s, File[] files, ArrayList<Integer> cl, String str) {
		this.socket = s;
		this.files = files;
		this.chunkList = cl;
		this.configClient = str;
	}

	public void run() {
		try {
			outStream = new ObjectOutputStream(socket.getOutputStream());

			outStream.writeObject(files.length);
			
			int clientNo = Character.getNumericValue(this.configClient.charAt(4));

			System.out.println("Sending total number of chunks to Client "+ clientNo);
			outStream.writeObject(chunkList.size());
			Arrays.sort(files);
			for (int i = 0; i < chunkList.size(); i++) {
				FileChunkObject sChunkObj = constructChuckFileObject(files[chunkList.get(i) - 1], chunkList.get(i));
				System.out.println("Sending chunk "+ ((i*5)+ clientNo) + " to Client "+ clientNo);
				sendChunkObject(sChunkObj);
				Thread.sleep(1000);
			}
			TCPDisconnectAsAServer(clientNo);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public FileChunkObject constructChuckFileObject(File file, int chunkNum) throws IOException {
		byte[] chunk = new byte[102400];
		FileChunkObject chunkObj = new FileChunkObject();

		chunkObj.setFileNum(chunkNum);

		chunkObj.setFileName(file.getName());
		FileInputStream fileInStream = new FileInputStream(file);

		BufferedInputStream bufferInStream = new BufferedInputStream(fileInStream);

		int bytesRead = bufferInStream.read(chunk);

		chunkObj.setChunksize(bytesRead);

		chunkObj.setFileData(chunk);

		bufferInStream.close();
		fileInStream.close();

		return chunkObj;
	}

	public void sendChunkObject(FileChunkObject sChunkObj) {
		try {

			outStream.writeObject(sChunkObj);
			outStream.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void TCPDisconnectAsAServer(int clientNo) {
		try {
			outStream.close();
			socket.close();
			System.out.println("Server connection to Client " + clientNo + " closed");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
