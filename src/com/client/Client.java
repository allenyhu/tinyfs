package com.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	private static Socket sock = null;
	private static ObjectOutputStream ostream;
	private static ObjectInputStream istream;
	/**
	 * Initialize the client
	 */
	public Client(){
		int port = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader("port.txt"));
			port = Integer.parseInt(br.readLine());
			System.out.println("port read in: " + port);
			if(this.sock == null) {
				this.sock = new Socket("localhost", port);
				this.ostream = new ObjectOutputStream(sock.getOutputStream());
				this.istream = new ObjectInputStream(sock.getInputStream());
			}
			
			System.out.println("Client connected on port: " + port);
		} catch(FileNotFoundException fnfe) {
			System.out.println("client fnfe: " + fnfe.getMessage());
		} catch(IOException ioe) {
			System.out.println("client on port " + port + " ioe: " + ioe.getMessage());
		} 
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		//read port number from file
		System.out.println("client initchunk");
		try {
			ostream.writeInt(ChunkServer.INIT);
			ostream.flush(); //sends init command
			System.out.println("client flushed");
			
			int size = istream.readInt();
			byte[] data = new byte[size];
			int count = 0;
			do {
				count += istream.read(data, count, size-count);
			} while(count != size);
			//byte[] data = ChunkServer.readBytes(istream, size);
			String handle = new String(data);
			System.out.println("handle from server: " + handle);
			return handle;
		} catch(IOException ioe) {
			return null;
		}
	}
	
	public void close() {
		try {
			this.sock.close();
		} catch(IOException ioe) {
			System.out.println("client close ioe: " + ioe.getMessage());
		}
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > ChunkServer.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		//command
		//offset
		//payload size
		//payload
		//handle size
		//handle
		try {
			ostream.writeInt(ChunkServer.PUT);
			ostream.writeInt(offset);
			ostream.writeInt(payload.length);
			System.out.println("payload size client: " + payload.length);
			ostream.write(payload);
			ostream.writeInt(ChunkHandle.getBytes().length);
			ostream.write(ChunkHandle.getBytes());
			ostream.flush();
			
			boolean success = istream.readBoolean();
			System.out.println("client read boolean");
			return success;
		} catch (IOException ioe) {
			System.out.println("Client putChunk ioe: " + ioe.getMessage());
		}
		return false;
		
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		System.out.println("client getChunk()");
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		//command
		//offset
		//num bytes
		//handle size
		//handle
		try {
			ostream.writeInt(ChunkServer.GET);
			ostream.writeInt(offset);
			ostream.writeInt(NumberOfBytes);
			ostream.writeInt(ChunkHandle.getBytes().length);
			ostream.write(ChunkHandle.getBytes());
			ostream.flush();
			
			int size = istream.readInt();
			System.out.println("size from server: " + size);
			byte[] data = ChunkServer.readBytes(this.istream, size);
			return data;
		} catch (IOException ioe) {
			System.out.println("Client getChunk ioe: " + ioe.getMessage());
		}
		return null;
	}

	


}
