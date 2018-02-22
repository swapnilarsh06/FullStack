import java.io.*;   
import java.net.*; 
import java.util.*;

class Client {

	public static void executePython(String inputFilePath) { 
		executePythonScript();
		createSocketAndListen(inputFilePath);
	}
	private static void executePythonScript(){
		try{
			Runtime r = Runtime.getRuntime();
			Process p = r.exec("python migrate_to_HDFS.py > pylog.txt");
			System.out.println("Python script executed successfully");
		} catch (Exception e) {
			String cause = e.getMessage();
			if (cause.equals("python: not found"))
				System.out.println("No python interpreter found.");
			System.out.println("Exception while executing python script: "+e);
		}
	}

	private static void createSocketAndListen(String inputFilePath){
		Socket rpiSocket = null; 
		DataInputStream in = null;
		PrintStream out = null;
		try {
			rpiSocket = new Socket("quickstart.cloudera",54310); 
			if(rpiSocket == null) {
				System.out.println("Null socket");
				System.exit(0);
			}			
			out = new PrintStream(rpiSocket.getOutputStream());
			BufferedInputStream bis = new BufferedInputStream(rpiSocket.getInputStream());
			if(bis == null) {
				System.out.println("Null bis");
				System.exit(0);
			}						
			in = new DataInputStream(bis);
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: hostname");
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: hostname"+e);
		}

		try {
			if (rpiSocket != null && out != null && in != null) {
				out.print(inputFilePath);
				while(true){
					byte[] bytes = new byte[1024];
					in.read(bytes);
					String reply = new String(bytes, "UTF-8");
					System.out.println("Reply from server: " + reply.trim());
					if(reply.trim().equals("EOD")){
						break;
					}
				}
			}
			rpiSocket.close();
			System.out.println("Connections closed successfully");
		}
		catch (IOException e) {
			System.err.println("IOException:  " + e);
		}

	}
}