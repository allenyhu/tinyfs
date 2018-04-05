package com.chunkserver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public static long counter;
	public static int INIT = 0;
	public static int PUT = 1;
	public static int GET = 2;
	public static int CMDBYTESIZE = 4;
	
	private ServerSocket ss = null;
	private Socket client = null;
	private ObjectInputStream istream;
	private ObjectOutputStream ostream;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
		int port = 6789;
		while(ss == null) {
			try {
				ss = new ServerSocket(port);
				BufferedWriter bw = new BufferedWriter(new FileWriter("port.txt"));
				bw.write(new Integer(port).toString());
				bw.flush();
				System.out.println("Chunkserver opened on port: " + ss.getLocalPort());
			} catch(IOException ioe) {
				System.out.println("server socket ioe: " + ioe.getMessage());
				port++;
				//System.out.println("trying on port: " + port);
			} 
		}
		while(true) {
			try {
				client = ss.accept();
				this.ostream = new ObjectOutputStream(client.getOutputStream());
				this.istream = new ObjectInputStream(client.getInputStream());
				System.out.println("client accepted");
					
				while(true) {
					int command = istream.readInt();
					System.out.println("command: " + command);
					if(command == ChunkServer.INIT) {
						System.out.println("got init message");
						String handle = this.initializeChunk();
						System.out.println("handle to be sent: " + handle);
						byte[] handleBytes = handle.getBytes();
						this.ostream.writeInt(handleBytes.length);
						this.ostream.write(handleBytes);
						this.ostream.flush();
					}
					else if(command == ChunkServer.PUT) {
						System.out.println("got put message");
						
						int offset = istream.readInt();
						int payloadSize = istream.readInt();
						byte[] payload = this.readBytes(this.istream, payloadSize);
						System.out.println("read payload");
						int handleSize = istream.readInt();
						System.out.println("read handlesize");
						byte[] handleBytes = this.readBytes(this.istream, handleSize);
						System.out.println("read handle");
						String handle = new String(handleBytes);
	
						boolean put = this.putChunk(handle, payload, offset);
						System.out.println("got boolean server");
						this.ostream.writeBoolean(put);
						this.ostream.flush();
					}
					else if(command == ChunkServer.GET) {
						System.out.println("got get message");
						//offset
						//num bytes
						//handle size
						//handle
						int offset = istream.readInt();
						int NumberOfBytes = istream.readInt();
						int handleSize = istream.readInt();
						byte[] handleBytes = this.readBytes(this.istream, handleSize);
						String handle = new String(handleBytes);
						byte[] data = this.getChunk(handle, offset, NumberOfBytes);
						
						this.ostream.writeInt(data.length);
						this.ostream.write(data);
						this.ostream.flush();
					}
				}
			} catch(IOException ioe) {
				System.out.println("server accept ioe: " + ioe.getMessage());
				
			}
		}
	}
	
	public static byte[] readBytes(ObjectInputStream ois, int size) {
		System.out.println("Server payload size: " + size);
		byte[] data = new byte[size];
		int count = 0;
		try {
			do {
				count += ois.read(data, count, size-count);
			} while(count != size);
		} catch (IOException ioe) {
			System.out.println("readBytes ioe: " + ioe.getMessage());
			return null;
		}
		System.out.println("count: " + count);
		return data;
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		//send this across the connection
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			System.out.println("server putChunk");
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		System.out.println("server getChunk");
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	public static void main(String[] args) {
		ChunkServer server = new ChunkServer();
	}
	
	

}
