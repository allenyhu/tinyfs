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
	final static String filePath = "csci485files/";	//or C:\\newfile.txt
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
				System.out.println("ServerSocket Established");
				BufferedWriter bw = new BufferedWriter(new FileWriter("port.txt"));
				bw.write(new Integer(port).toString());
				bw.flush();
			} catch(IOException ioe) {
				System.out.println("server socket ioe: " + ioe.getMessage());
				port++;
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
					if(command == ChunkServer.INIT) {
						String handle = this.initializeChunk();
						byte[] handleBytes = handle.getBytes();
						this.ostream.writeInt(handleBytes.length);
						this.ostream.write(handleBytes);
						this.ostream.flush();
					}
					else if(command == ChunkServer.PUT) {
						//command
						//offset
						//payload size
						//payload
						//handle size
						//handle
						int offset = istream.readInt();
						int payloadSize = istream.readInt();
						byte[] payload = this.readBytes(this.istream, payloadSize);
						int handleSize = istream.readInt();
						byte[] handleBytes = this.readBytes(this.istream, handleSize);
						String handle = new String(handleBytes);
	
						boolean put = this.putChunk(handle, payload, offset);
						this.ostream.writeBoolean(put);
						this.ostream.flush();
					}
					else if(command == ChunkServer.GET) {
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
	
	/*
	 * Reads in a specified number of bytes from the give ObjectInputStream
	 */
	public static byte[] readBytes(ObjectInputStream ois, int size) {
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
		return data;
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
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
