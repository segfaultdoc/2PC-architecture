package edu.gmu.cs475;

import edu.gmu.cs475.struct.NoSuchTagException;

import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.*;

import java.util.concurrent.locks.*;

public class FileManagerClient extends AbstractFileManagerClient {
  // client cache that maps files to content
  private HashMap <String, String> readOnlyReplica  = new HashMap <String, String> ();
  // pending transaction that maps xid's to a File <> Content HashMap
  private HashMap <Long, HashMap <String, String> > notVisibleWrites = new HashMap <Long, HashMap <String, String> > ();
  private ReadWriteLock lock = new ReentrantReadWriteLock(); 

  public FileManagerClient(String host, int port) {
		super(host, port);
		startReplica();
	}

	/**
	 * Used for tests without a real server
	 *
	 * @param server
	 */
	public FileManagerClient(IFileManagerServer server) {
		super(server);
		startReplica();
	}

	/**
	 * Initialzes this read-only replica with the current set of files
	 *
	 * @param files A map from filename to file contents
	 */
	@Override
	protected void initReplica(HashMap<String, String> files) {
    for(String key: files.keySet()){
      this.readOnlyReplica.put(key, files.get(key));
    }

	}

	/**
	 * Lists all of the paths to all of the files that are known to the client
	 *
	 * @return the file paths
	 */
	@Override
	public Iterable<String> listAllFiles() {
		this.lock.readLock().lock();
    try{
      ArrayList <String> files = new ArrayList <String>();
      for(String p: this.readOnlyReplica.keySet()){
        files.add(new String(p));
      }
      return files;
    } finally{ this.lock.readLock().unlock(); }
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
	public String catAllFiles() throws IOException {
    ArrayList <String> fileArray = new ArrayList <String> (this.readOnlyReplica.keySet());
    Collections.sort(fileArray);
    StringBuilder sb = new StringBuilder();
    ArrayList <Long> stamps = new ArrayList<Long> ();
    
    for(String f: fileArray){ 
      stamps.add(new Long(super.lockFile(f, false)));
    }
    int count = 0;
    for(String f: fileArray){
      try{ 
        sb.append(this.readFile(f));
      } catch(IOException e){
          e.printStackTrace();
      } catch(Throwable t){
          t.printStackTrace();
      } finally{
          super.unLockFile(f, stamps.get(count).longValue(), false);
          count++;
      }
    }
    return sb.toString();
	}

	/**
	 * Echos some content into all files that have a given tag. Must internally
	 * synchronize to guarantee that the list of files with the given tag does
	 * not change during its call, and that each file being printed to does not
	 * change during its execution (using a read/write lock)
	 * <p>
	 * Given two concurrent calls to echoToAllFiles, it will be indeterminate
	 * which call happens first and which happens last. But what you can (and
	 * must) guarantee is that all files will have the *same* value (and not
	 * some the result of the first, qnd some the result of the second). Your
	 * could should not deadlock while waiting to acquire locks.
	 *
	 * @param tag     Tag to query for
	 * @param content The content to write out to each file
	 * @throws NoSuchTagException If no tag exists with the given name
	 * @throws IOException        if any IOException occurs in the underlying write, or if the
	 *                            write was unsuccessful (e.g. if it times out, or gets
	 *                            otherwise disconnected during the execution)
	 */
  @Override
	public void echoToAllFiles(String content) throws NoSuchTagException, IOException {
    ArrayList <String> fileArray = new ArrayList <String> (this.readOnlyReplica.keySet()); 
    Collections.sort(fileArray);
    ArrayList <Long> stamps = new ArrayList<Long> ();
    
    for(String f: fileArray){
      stamps.add(new Long(super.lockFile(f, true)));
    }
    boolean bool = true;
    long xid = 0;
    int count = 0;
    xid = super.startNewTransaction();
    for(String f: fileArray){
      try{ 
        bool = bool && super.writeFileInTransaction(f, content, xid);
      } 
      catch(Exception e){
      } 
      finally {
          super.unLockFile(f, stamps.get(count).longValue(), true);
          count++;
      }
    }
    
    if(bool){ super.issueCommitTransaction(xid); }
    else { super.issueAbortTransaction(xid); }
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
		this.lock.readLock().lock();
    try{
	    synchronized(this.readOnlyReplica){
        return this.readOnlyReplica.get(file);
      }
    } finally { this.lock.readLock().unlock(); }
    
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
      HashMap <String, String> h = new HashMap <String, String> ();
      h.put(file, content);
      // if xid is not zero then the write is not visible
      if(xid != 0){
        // synchronizes on the datastructure so no
        // other method can modify it
        synchronized(this.notVisibleWrites){
          this.notVisibleWrites.put(new Long(xid), h);
          return true;
        }
      }
    // else it is visible
    else{
      synchronized(this.readOnlyReplica){
        this.readOnlyReplica.put(file, content);
        return true;
      }
    } 
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
    HashMap <String, String> h  = null;
    synchronized(this.notVisibleWrites){  
      h = this.notVisibleWrites.get(id);
    }
    synchronized(this.readOnlyReplica){
      // adds the write to cache
      if(h != null){ 
        for(String file: h.keySet()){
          this.readOnlyReplica.put(file, h.get(file)); 
        }
        
      }
    }
    // removes it from the notvisible data structure
    synchronized(this.notVisibleWrites){
      this.notVisibleWrites.remove(id);
    }
  }

	/**
	 * Abort a transaction, discarding any pending writes that are associated with it
	 *
	 * @param id transaction id
	 * @throws RemoteException
	 */
	@Override
	public void abortTransaction(long id) throws RemoteException { 
    synchronized(this.notVisibleWrites){
      this.notVisibleWrites.remove(id);
    }
  }
}

