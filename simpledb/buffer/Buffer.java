package simpledb.buffer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 * 
 * @author Modified by Kaustubh Sant to implement Generic Clock replacement policy
 * @author Modified by Nupur Mallik to add the saveBlock and the restoreBlock methods
 */

public class Buffer {
   private Page contents = new Page();
   private Block blk = null;
   private int pins = 0;
   private int refbit;	//reference counter
   private int refcounter;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding log record
   //File saveFilename = new File("C:\\Users\\Nupur\\Softwares\\saveBlock.txt");
   String saveFilename= "MyFile.txt";
   PageFormatter fmtr1 = null;
   static ArrayList<Integer> al = new ArrayList<Integer>();

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   public Buffer(int refcount) {
	   refbit = -1;
	   refcounter = refcount;
   }
   
   
   public int getPins() {
	return pins;
   }


   public void setPins(int pins) {
	this.pins = pins;
   }


   public int getRefbit() {
	return refbit;
   }


   public void setRefbit(int refbit) {
	this.refbit = refbit;
   }


/**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
   }

   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   void flush() {
      if (modifiedBy >= 0) {
    	 Block newblk = saveBlock(blk);
         SimpleDB.logMgr().flush(logSequenceNumber);
         if(al.size() != 0)
         {
        	 al.remove(blk.number());
         }
         contents.write(blk);
         modifiedBy = -1;
      }
   }

   /**
    * Increases the buffer's pin count.
    */
   void pin() {
      pins++;
      if(pins > 0)
    	  refbit =-1;
   }

   /**
    * Decreases the buffer's pin count.
    */
   void unpin() {
      pins--;
      if(pins==0)
    	  refbit = refcounter;
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   boolean isPinned() {
      return pins > 0;
   }
   
   /**
    * Returns true if the buffer has reference counter is non-zero.
    * @return true if the buffer is pinned
    */
   
   boolean isRefbit() {
	   if(refbit>0){
		   refbit--;
		   return true;
	   }
	   else
		   return false;
   }
   
   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
      refbit =-1;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
      refbit = -1;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and copies the contents of the buffer to a new block in the file of saved blocks.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param fmtr a page formatter, used to initialize the page
    */
   public Block saveBlock(Block blk) {
	   Block newblk = null;
	   int count = 0;
	   for(int i=0; i<al.size(); i++)
	   {
		   if(al.get(i) == blk.number())
			   count = 1;
	   }
	   if ((modifiedBy >= 0) && (count != 1)) 
	   {
	       //fmtr.format(contents);
	       newblk = contents.append(saveFilename);
	       al.add(newblk.number());
	   }
	   return newblk;
   }
   
   /**
    * Initializes the buffer's page according to the specified formatter,
    * and copies the contents of the buffer to a new block in the file of saved blocks.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
    public void restoreBlock(int txnum, int lsn) {
	   try{
	      modifiedBy = txnum;
	      int blknum = 0;
	      FileReader fileReader = new FileReader("simpledb.log");
	      Scanner scanner = new Scanner(fileReader);
	      while(scanner.hasNext())
	      {
	    	  int txn = scanner.nextInt();
	    	  if (txn == txnum)
	    	  {
	    		  blknum = scanner.nextInt();	    		  
	    		  String s = "MyFile.txt";
	    		  FileReader fileReader1 = new FileReader(s);
	    	
	    		  
	    	      Scanner scanner1 = new Scanner(fileReader1);
	    	      while(scanner1.hasNext())
	    	      {
	    	    	  int blkno = scanner1.nextInt();
	    	    	  if (blkno == blknum)
	    	    	  {
	    	    		  Block block = new Block("MyFile.txt",blkno);
	    	    		  assignToBlock(block);
	    	    	  }
	    	      }
	    	      scanner1.close();
	    	  }
	      }
	      scanner.close();
	      if (lsn >= 0)
		      logSequenceNumber = lsn;
     }
   catch (IOException e) {
      throw new RuntimeException("cannot read block ");
   }
	   }

}