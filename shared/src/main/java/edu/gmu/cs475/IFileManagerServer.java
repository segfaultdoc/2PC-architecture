package edu.gmu.cs475;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.NoSuchFileException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

import edu.gmu.cs475.struct.NoSuchTagException;
import edu.gmu.cs475.struct.TagExistsException;

public interface IFileManagerServer extends Remote {
	public static final String RMI_NAME = "cs475IFileManagerServer";

	/**
	 * Write (or overwrite) a file.
	 * You must not allow a client to register or depart during a write.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	public void writeFile(String file, String content) throws RemoteException, IOException;

	/**
	 * Write (or overwrite) a file. Broadcasts the write to all replicas and locally on the server
	 * You must not allow a client to register or depart during a write.
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID to use for any replications of this write
	 * @return True if all replicas replied "OK" to this write
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	public boolean writeFileInTransaction(String file, String content, long xid) throws RemoteException, IOException;

	/**
	 * Acquires a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param forWrite True if a write lock is requested, else false
	 * @return A stamp representing the lock owner (e.g. from a StampedLock)
	 * @throws NoSuchFileException If the file doesn't exist
	 */
	public long lockFile(String name, boolean forWrite) throws RemoteException, NoSuchFileException;

	/**
	 * Releases a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param stamp    the Stamp representing the lock owner (returned from lockFile)
	 * @param forWrite True if a write lock is requested, else false
	 * @throws NoSuchFileException          If the file doesn't exist
	 * @throws IllegalMonitorStateException if the stamp specified is not (or is no longer) valid
	 */
	public void unLockFile(String name, long stamp, boolean forWrite) throws RemoteException, NoSuchFileException, IllegalMonitorStateException;


	/**
	 * Registers a cache with the server, returning all of the files that currently exist.
	 *
	 * @param hostname   The hostname that should be used to contact this cache (sent again on disconnect)
	 * @param portNumber The port number to use to contact the cache (sent again on disconnect)
	 * @return A HashMap of all of the files that currently exist, mapping from filepath/name to its contents
	 * @throws RemoteException
	 */
	public HashMap<String, String> registerClient(String hostname, int portNumber) throws RemoteException;

	/**
	 * Notifies the server that a cache client is shutting down (and hence no longer will be involved in writes)
	 *
	 * @param hostname   The hostname of the client that is disconnecting (same hostname specified when it registered)
	 * @param portNumber The port number of the client that is disconnecting (same port number specified when it registered)
	 * @throws RemoteException
	 */
	public void cacheDisconnect(String hostname, int portNumber) throws RemoteException;

	/**
	 * Request a new transaction ID to represent a new, client-managed transaction
	 *
	 * @return Transaction organizer-provided ID that will be used in the future to commit or abort this transaction
	 */
	public long startNewTransaction() throws RemoteException;

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be committed
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 * @throws IOException in case of any underlying IOException upon commit
	 */
	public void issueCommitTransaction(long xid) throws RemoteException, IOException;

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be aborted
	 * You must not allow a client to register or depart during an abort.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 */
	public void issueAbortTransaction(long xid) throws RemoteException;
}
