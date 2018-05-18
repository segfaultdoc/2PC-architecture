package edu.gmu.cs475;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;

import java.util.concurrent.locks.*;

public class FileManagerServer extends AbstractFileManagerServer {
	
  // Maps TaggedFile objects to their names'
  private HashMap <String, TaggedFile> fileMap = new HashMap <String, TaggedFile> ();
  // maps Port number to hashmap that maps hostname to IFileReplica Objects
  private HashMap <Integer, HashMap <String, IFileReplica> > clients = new HashMap <Integer, HashMap <String, IFileReplica> > ();
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Random r = new Random(); 
  
  /**
	 * Initializes the server with the state of the files it cares about
	 *
	 * @param files list of the files
	 */
	public void init(List<Path> files) {
    TaggedFile f = null;
    String content = null;
    for(Path p: files){
      try{
        content = super.readFileLocally(p.toString());
      } catch(IOException e){ 
        
      }
      f = new TaggedFile(p, content);
      this.fileMap.put(p.toString(), f);
    }
	}

	/**
	 * Registers a replica with the server, returning all of the files that currently exist.
	 *
	 * @param hostname   the hostname of the replica talking to you (passed again at disconnect)
	 * @param portNumber the port number of the replica talking to you (passed again at disconnect)
	 * @param replica    The RMI object to use to signal to the replica
	 * @return A HashMap of all of the files that currently exist, mapping from filepath/name to its contents
	 * @throws IOException in case of an underlying IOExceptino when reading the files
	 */
	@Override
	public HashMap<String, String> registerReplica(String hostname, int portNumber, IFileReplica replica) throws IOException {
	  this.lock.writeLock().lock();
    try{
      // sets up a hashmap of hostname to IFileReplica to put into the clients HashMap
      HashMap <String, IFileReplica> h = new HashMap <String, IFileReplica> ();
      h.put(hostname, replica);
      HashMap <String, IFileReplica> tmp = new HashMap <String, IFileReplica> ();
      // checks to see if this.clients already contains the port number key
      if(this.clients.containsKey(portNumber)){
        // retreives the hashmap, mapped to this port number
        tmp = this.clients.get(portNumber);
        // adds this key-value pair to the HashMap value mapped to the port number
        tmp.put(hostname, replica);
      }
      else{
        // if this port number is not already a key in the clients map, then it will be added
        this.clients.put(new Integer(portNumber), h);
      }
      HashMap <String, String> ret = new HashMap <String, String> ();
      // iterates through the files and gets their contents, to return
      for(TaggedFile f: this.fileMap.values()){ 
        ret.put(f.getName(), super.readFileLocally(f.getName()));
      }
      return ret;
    } finally { this.lock.writeLock().unlock(); }
	}

	/**
	 * Write (or overwrite) a file.
	 * You must not allow a client to register or depart during a write.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	@Override
	public void writeFile(String file, String content) throws RemoteException, IOException {
    long stamp = 0;
    long xid = 0; 
    // a read lock so that this method can be called concurrently
    this.lock.readLock().lock();
    try{
      // locks the file
      stamp = this.lockFile(file, true);
  
      if(!this.fileMap.containsKey(file)){  
        this.fileMap.put(file, new TaggedFile(file, content));
        super.writeFileLocally(file, content);
      }
      HashMap <String, IFileReplica> fileToIFile = null; 
      boolean bool = true;       
      xid = this.startNewTransaction();
      // if writeFileInTransaction is successful, then commit
      // is issued, otherwise, it will throw an IOException
      if(this.writeFileInTransaction(file, content, xid)){
        this.issueCommitTransaction(xid);
        this.fileMap.get(file).setContent(content);
      }
      else { 
        throw new IOException(); 
      }
    } 
    catch(IOException e) {
      // aborts transaction 
      this.issueAbortTransaction(xid);
      throw new IOException();
    }
    finally { 
      this.unLockFile(file, stamp, true);
      this.lock.readLock().unlock(); 
    }
	}

	/**
	 * Write (or overwrite) a file. Broadcasts the write to all replicas and locally on the server
	 * You must not allow a client to register or depart during a write.
	 *
	 * @param file    Path to file to write out
	 * @param content String representing the content desired
	 * @param xid     Transaction ID to use for any replications of this write
	 * @return True if all replicas replied "OK" to this write
	 * @throws IOException if any IOException occurs in the underlying write, OR if the write is not succesfully replicated
	 */
	@Override
	public boolean writeFileInTransaction(String file, String content, long xid) throws RemoteException, IOException {
		// readlock so that this method can be called concurrently
    this.lock.readLock().lock();
    long stamp = 0;
    try{
      
      if(!this.fileMap.containsKey(file)){
        this.fileMap.put(file, new TaggedFile (file, content));
        super.writeFileLocally(file, content);
        return true;
      }
      else{
        
      
      boolean ret = true;
      Collection < HashMap <String, IFileReplica> > coll = this.clients.values();
      Iterator < HashMap <String, IFileReplica> > itr = coll.iterator(); 
      // iterates through all HashMap values, mapped to all unique ports
      while(itr.hasNext()){
        HashMap <String, IFileReplica> hostMap = itr.next();
        // iterates through all RMI objects to call innerwrite
        for(IFileReplica f: hostMap.values()){
          // all returns by innerWriteFile() must be true
          // in order for ret to be true
          ret = ret && f.innerWriteFile(file, content, xid);
        }
      }
     
      if(ret){
        // only writes the file on the server if all clients
        // innerwrote successfully
        super.writeFileLocally(file, content);
      }
      return ret;
    }
    } catch(Exception e){ 
        return false;
      } 
    finally {  
      this.lock.readLock().unlock();  
    } 
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
		TaggedFile f = null;
    synchronized(this.fileMap){
      f = this.fileMap.get(name);
      if(f == null){ throw new NoSuchFileException("This file does not exist"); }
    }
    long stamp;
    if(forWrite == true){  
      stamp = f.getLock().writeLock(); 
      f.setWriteLock(stamp);
    }
    else { 
      stamp = f.getLock().readLock();  
      f.addReadLock(new Long(stamp));
    }
    return stamp;
    
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
	@Override
	public void unLockFile(String name, long stamp, boolean forWrite) throws RemoteException, NoSuchFileException, IllegalMonitorStateException {
    
    TaggedFile f = null;
    synchronized(this.fileMap){
      f = this.fileMap.get(name);
      if(f == null){ throw new NoSuchFileException("This file does not exist"); }
    } 
    f.getLock().unlockWrite(stamp);
    f.setWriteLock(0); 
  }
	/**
	 * Notifies the server that a cache client is shutting down (and hence no longer will be involved in writes)
	 *
	 * @param hostname   The hostname of the client that is disconnecting (same hostname specified when it registered)
	 * @param portNumber The port number of the client that is disconnecting (same port number specified when it registered)
	 * @throws RemoteException
	 */
	@Override
	public void cacheDisconnect(String hostname, int portNumber) throws RemoteException {
    // write lock, because we don't want anything else to happen
    // on the server while a client disconnects and visa versa
    this.lock.writeLock().lock();
    try{
        // removes the client associated with this port and host
        HashMap <String, IFileReplica> h = this.clients.get(portNumber);
        if(h != null)
        {  
          h.remove(hostname); 
        }
    
    }
     finally { this.lock.writeLock().unlock(); }
	}

	/**
	 * Request a new transaction ID to represent a new, client-managed transaction
	 *
	 * @return Transaction organizer-provided ID that will be used in the future to commit or abort this transaction
	 */
	@Override
	public long startNewTransaction() throws RemoteException {
		return r.nextLong();
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be committed
	 * You must not allow a client to register or depart during a commit.
	 *
	 * @param xid transaction ID to be committed (from startNewTransaction)
	 * @throws IOException in case of any underlying IOException upon commit
	 */
	@Override
	public void issueCommitTransaction(long xid) throws RemoteException, IOException {
    // readlock so that this method can be called concurrently  
    this.lock.readLock().lock();
    try{
      // calles commit transaction on all RMI objects
      Collection < HashMap  <String, IFileReplica> > h = this.clients.values();
      Iterator <HashMap <String, IFileReplica> > itr = h.iterator();
      while(itr.hasNext()){
        HashMap <String, IFileReplica> map = itr.next();
        for(IFileReplica f: map.values()){
          f.commitTransaction(xid);
        }
      }
    } finally { this.lock.readLock().unlock(); }
  
	}

	/**
	 * Broadcast to all replicas (and make updates locally as necessary on the server) that a transaction should be aborted
	 * You must not allow a client to register or depart during an abort.
	 *
	 * @param xid transaction ID to be aborted (from startNewTransaction)
	 */
	@Override
	public void issueAbortTransaction(long xid) throws RemoteException {
    // read lock so this method can be called concurrently
    this.lock.readLock().lock();
      try{
        Collection < HashMap  <String, IFileReplica> > h = this.clients.values();
        Iterator <HashMap <String, IFileReplica> > itr = h.iterator();
        // calls abort transaction on all RMI objects
        while(itr.hasNext()){
          HashMap <String, IFileReplica> map = itr.next();
          for(IFileReplica f: map.values()){
            f.abortTransaction(xid);
          }
        }
      } finally { this.lock.readLock().unlock(); }

	}
}
