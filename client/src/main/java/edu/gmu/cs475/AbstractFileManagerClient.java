package edu.gmu.cs475;

import edu.gmu.cs475.struct.NoSuchTagException;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractFileManagerClient implements IFileReplica {

	private IFileManagerServer tagServer;

	private int localPort;
	private Registry rmiRegistry;

	protected void startReplica(){
		try {
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				localPort = socket.getLocalPort();
			}
			rmiRegistry = LocateRegistry.createRegistry(localPort);
			IFileReplica replica = (IFileReplica) UnicastRemoteObject.exportObject(this,0);
			rmiRegistry.rebind(IFileReplica.RMI_NAME,replica);
			System.out.println("Bound replica to " + localPort);
			initReplica(tagServer.registerClient("localhost",localPort));
		}catch(IOException ex)
		{
			ex.printStackTrace();
		}
	}
	protected AbstractFileManagerClient(String host, int port)
	{
		try {
			Registry registry = LocateRegistry.getRegistry(host, port);
			tagServer = (IFileManagerServer) registry.lookup(IFileManagerServer.RMI_NAME);
		} catch (Exception e) {
			System.err.println("Client exception connecting to lock server: " + e.toString());
			e.printStackTrace();
		}
	}
	protected AbstractFileManagerClient(IFileManagerServer server){
		this.tagServer = server;
	}


	/**
	 * Cleans up the RMI sever that's running the cache client
	 */
	public final void cleanup()
	{
		try {
			tagServer.cacheDisconnect("localhost",localPort);
			UnicastRemoteObject.unexportObject(this,true);
			rmiRegistry.unbind(IFileReplica.RMI_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initialzes this read-only replica with the current set of files
	 * @param files A map from filename to file contents
	 */
	protected abstract void initReplica(HashMap<String,String> files);

	/**
	 * Lists all of the paths to all of the files that are known to the client
	 *
	 * @return the file paths
	 */
	public abstract Iterable<String> listAllFiles();

	/**
	 * Write (or overwrite) a file.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	public void writeFile(String file, String content) throws RemoteException, IOException {
		tagServer.writeFile(file, content);
	}

	/**
	 * Acquires a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param forWrite True if a write lock is requested, else false
	 * @return A stamp representing the lock owner (e.g. from a StampedLock)
	 * @throws NoSuchFileException If the file doesn't exist
	 */
	public long lockFile(String name, boolean forWrite) throws RemoteException, NoSuchFileException {
		return tagServer.lockFile(name, forWrite);
	}


	/**
	 * Releases a read or write lock for a given file.
	 *
	 * @param name     File to lock
	 * @param stamp    the Stamp representing the lock owner (returned from lockFile)
	 * @param forWrite True if a write lock is requested, else false
	 * @throws NoSuchFileException          If the file doesn't exist
	 * @throws IllegalMonitorStateException if the stamp specified is not (or is no longer) valid
	 */
	public void unLockFile(String name, long stamp, boolean forWrite) throws RemoteException, NoSuchFileException, IllegalMonitorStateException {
		tagServer.unLockFile(name, stamp, forWrite);
	}


	/**
	 * Start a transaction.
	 *
	 * @return Transaction organizer-provided ID that will be used in the future to commit or abort this transaction
	 * @throws RemoteException
	 */
	public long startNewTransaction() throws RemoteException {
		return tagServer.startNewTransaction();
	}

	public void issueCommitTransaction(long xid) throws IOException {
		tagServer.issueCommitTransaction(xid);
	}

	/**
	 * Write (or overwrite) a file.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID to use for any replications of this write
	 * @return True if all replicas replied "OK" to this write
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	public boolean writeFileInTransaction(String file, String content, long xid) throws RemoteException, IOException {
		return tagServer.writeFileInTransaction(file, content, xid);
	}

	public void issueAbortTransaction(long xid) throws RemoteException {
		tagServer.issueAbortTransaction(xid);
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
	 * @throws NoSuchTagException
	 *             If no tag exists with the given name
	 * @throws IOException
	 *             if any IOException occurs in the underlying read, or if the
	 *             read was unsuccessful (e.g. if it times out, or gets
	 *             otherwise disconnected during the execution
	 *
	 */
	public abstract String catAllFiles() throws NoSuchTagException, IOException;

	/**
	 * Echos some content into all files. Must internally
	 * synchronize to guarantee that the list of files does
	 * not change during its call, and that each file being printed to does not
	 * change during its execution (using a read/write lock)
	 *
	 * Given two concurrent calls to echoToAllFiles, it will be indeterminate
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all files will have the *same* value (and not
	 * some the result of the first, qnd some the result of the second). Your
	 * code should not deadlock while waiting to acquire locks.
	 *
	 * Must use a transaction to guarantee that all writes succeed to all replicas (or none).
	 *
	 * @param content
	 *            The content to write out to each file
	 * @throws NoSuchTagException
	 *             If no tag exists with the given name
	 * @throws IOException
	 *             if any IOException occurs in the underlying write, or if the
	 *             write was unsuccessful (e.g. if it times out, or gets
	 *             otherwise disconnected during the execution)
	 *
	 */
	public abstract void echoToAllFiles(String content) throws IOException;
}
