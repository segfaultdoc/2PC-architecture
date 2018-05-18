package edu.gmu.cs475;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.locks.StampedLock;
import java.util.concurrent.*;

public class TaggedFile {
	public HashMap<String, IFileReplica> tags = new HashMap<String, IFileReplica>();
  private Path path;
	private StampedLock lock = new StampedLock();
  private ArrayList <Long> readLocks = new ArrayList <Long> ();
  private long writeLock = 0;
  private String content;
  private String pathStr;

  public TaggedFile(String path, String content){
    this.pathStr = path;
    this.path = null;
    this.content = content;
  }

  
  public TaggedFile(Path path, String content){
    this.path = path;
    this.content = content;

  }

  public TaggedFile(Path path){
    this.path = path;
    this.content = null;
  }

  public String getContent(){
    return this.content;
  }

  public synchronized void setContent(String content){
    this.content = content;
  }

  public synchronized void setWriteLock(long stamp){
    this.writeLock = stamp;
  }
  public synchronized long getWriteLock(){
    return this.writeLock;
  }
  public synchronized void addReadLock(Long lock){
    this.readLocks.add(lock);
  }
  public synchronized void removeReadLock(long lock){
   try{
    for(Long l: this.readLocks){
      if(l.longValue() == lock){ 
        this.readLocks.remove(l); 
      }
    }
   }
    catch(Exception e){ 
    }
    finally{
      //if(this.readLocks.size() == 0){
      this.getLock().unlockRead(lock);
     // }
    }
  }
  public synchronized ArrayList<Long> getReadLocks(){
    return new ArrayList <Long> (this.readLocks);
  }
  public synchronized boolean hasReadLock(long stamp){
    
    for(Long l: readLocks){
      if(l.longValue() == stamp){ return true; }
    }
    return false;

  }
  public StampedLock getLock(){
    return this.lock;
  }	
  
	public String getName() {
		return path.toString();
	}

	public String toString() {
		return getName();
	}

}
