package com.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	//public static ChunkServer cs = new ChunkServer();
	private Socket sock;
	private ObjectOutputStream ostream;
	private ObjectInputStream istream;
	/**
	 * Initialize the client
	 */
	public Client(){
		Scanner scan = new Scanner(System.in);
		System.out.print("Please enter the port to connect to: " );
		int port = scan.nextInt();
		try {
			this.sock = new Socket("localhost", port);
			this.ostream = new ObjectOutputStream(sock.getOutputStream());
			this.istream = new ObjectInputStream(sock.getInputStream());
			
			System.out.println("Client connected on port: " + port);
		} catch(IOException ioe) {
			System.out.println("client on port " + port + " ioe: " + ioe.getMessage());
		}
		scan.close();
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
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
			return new String(data);
		} catch(IOException ioe) {
			return null;
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
		//return cs.putChunk(ChunkHandle, payload, offset);
		return false;
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		//return cs.getChunk(ChunkHandle, offset, NumberOfBytes);
		return null;
	}

	


}
