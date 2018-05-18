package edu.gmu.cs475.internal;

import edu.gmu.cs475.AbstractFileManagerClient;
import edu.gmu.cs475.IFileManagerServer;
import edu.gmu.cs475.struct.NoSuchTagException;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;

public class NoOpFileManagerClient extends AbstractFileManagerClient {
	public NoOpFileManagerClient(IFileManagerServer server){
		super(server);
	}
	/**
	 * Initialzes this read-only replica with the current set of files
	 *
	 * @param files A map from filename to file contents
	 */
	@Override
	protected void initReplica(HashMap<String, String> files) {
		throw new AssertionError("Unexpected method call");
	}

	/**
	 * Lists all of the paths to all of the files that are known to the client
	 *
	 * @return the file paths
	 */
	@Override
	public Iterable<String> listAllFiles() {
		throw new AssertionError("Unexpected method call");
	}

	/**
	 * Prints out all files. Must internally synchronize
	 * to guarantee that the list of files does not change
	 * during its call, and that each file printed does not change during its
	 * execution (using a read/write lock). You should acquire all of the locks,
	 * then read all of the files and release the locks. Your code should not
	 * deadlock while waiting to acquire locks.
	 *
	 * @return The concatenation of all of the files
	 * @throws NoSuchTagException If no tag exists with the given name
	 * @throws IOException        if any IOException occurs in the underlying read, or if the
	 *                            read was unsuccessful (e.g. if it times out, or gets
	 *                            otherwise disconnected during the execution
	 */
	@Override
	public String catAllFiles() throws NoSuchTagException, IOException {
		throw new AssertionError("Unexpected method call");
	}

	/**
	 * Echos some content into all files. Must internally
	 * synchronize to guarantee that the list of files does
	 * not change during its call, and that each file being printed to does not
	 * change during its execution (using a read/write lock)
	 * <p>
	 * Given two concurrent calls to echoToAllFiles, it will be indeterminate
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all files will have the *same* value (and not
	 * some the result of the first, qnd some the result of the second). Your
	 * code should not deadlock while waiting to acquire locks.
	 * <p>
	 * Must use a transaction to guarantee that all writes succeed to all replicas (or none).
	 *
	 * @param content The content to write out to each file
	 * @throws NoSuchTagException If no tag exists with the given name
	 * @throws IOException        if any IOException occurs in the underlying write, or if the
	 *                            write was unsuccessful (e.g. if it times out, or gets
	 *                            otherwise disconnected during the execution)
	 */
	@Override
	public void echoToAllFiles(String content) throws IOException {
		throw new AssertionError("Unexpected method call");
	}

	/**
	 * Return a file as a byte array.
	 *
	 * @param file Path to file requested
	 * @return String representing the file
	 * @throws IOException if any IOException occurs in the underlying read
	 */
	@Override
	public String readFile(String file) throws RemoteException, IOException {
		throw new AssertionError("Unexpected method call");
	}

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
	@Override
	public boolean innerWriteFile(String file, String content, long xid) throws RemoteException, IOException {
		throw new AssertionError("Unexpected method call");
	}

	/**
	 * Commit a transaction, making any pending writes immediately visible
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 * @throws IOException     if any IOException occurs in the underlying write
	 */
	@Override
	public void commitTransaction(long id) throws RemoteException, IOException {
		throw new AssertionError("Unexpected method call");

	}

	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	@Override
	public void abortTransaction(long id) throws RemoteException {
		throw new AssertionError("Unexpected method call");

	}
}
