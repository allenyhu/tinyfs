package com.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
	private OutputStream ostream;
	private InputStream istream;
	/**
	 * Initialize the client
	 */
	public Client(){
//		if (cs == null)
//			cs = new ChunkServer();
		
		Scanner scan = new Scanner(System.in);
		System.out.print("Please enter the port to connect to: " );
		int port = scan.nextInt();
		try {
			this.sock = new Socket("localhost", port);
			this.ostream = sock.getOutputStream();
			this.istream = sock.getInputStream();
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
		//return cs.initializeChunk();
		try {
			PrintWriter pw = new PrintWriter(this.ostream);
			BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
			pw.print("init");
			pw.flush();
			return br.readLine();
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
	}

	


}
