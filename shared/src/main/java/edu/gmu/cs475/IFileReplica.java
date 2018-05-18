package edu.gmu.cs475;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IFileReplica extends Remote {
	public static final String RMI_NAME = "cs475IFileManagerReplica";

	/**
	 * Return a file as a byte array.
	 *
	 * @param file Path to file requested
	 * @return String representing the file
	 * @throws IOException if any IOException occurs in the underlying read
	 */
	public String readFile(String file) throws RemoteException, IOException;

	/**
	 * Write (or overwrite) a file
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID, if this write is associated with any transaction, or 0 if it is not associated with a transaction
	 *                If it is associated with a transaction, then this write must not be visible until the replicant receives a commit message for the associated transaction ID; if it is aborted, then it is discarded.
	 * @return true if the write was successful and we are voting to commit
	 * @throws IOException if any IOException occurs in the underlying write
	 */
	public boolean innerWriteFile(String file, String content, long xid) throws RemoteException, IOException;

	/**
	 * Commit a transaction, making any pending writes immediately visible
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 * @throws IOException     if any IOException occurs in the underlying write
	 */
	public void commitTransaction(long id) throws RemoteException, IOException;


	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	public void abortTransaction(long id) throws RemoteException;
}
