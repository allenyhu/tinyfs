package com.chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "U:\\Desktop\\csci485Disk\\"; // or C:\\newfile.txt
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		//System.out.println(
		//		"Constructor of ChunkServer is invoked:  Part 1 of TinyFS must implement the body of this method.");
		counter = 0;
	}

	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file.
	 */
	public String initializeChunk() {
//		System.out.println("createChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
//		System.out.println("Returns null for now.\n");
		File chunk = new File(filePath + counter);
		return chunk.getPath();
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			RandomAccessFile writer = new RandomAccessFile(ChunkHandle, "rw");
		    writer.seek(offset);
		    writer.write(payload);
		    writer.close();
		} catch (IOException ioe) {
			System.out.println("Failed to putChunk");
			System.out.println("error: " + ioe.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		byte[] result = new byte[NumberOfBytes];
		try {
		    RandomAccessFile reader = new RandomAccessFile(ChunkHandle, "r");
		    reader.read(result, offset, NumberOfBytes);
		    reader.close();
		} catch (IOException ioe) {
			System.out.println("Failed getChunk");
			System.out.println("error: " + ioe.getMessage());
			return null;
		}
	    return result;
	}

}
