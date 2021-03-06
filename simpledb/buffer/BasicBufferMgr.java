package simpledb.buffer;

import simpledb.file.*;
import java.util.HashMap;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 * 
 * @author Modified by Kaustubh Sant to implement Generic Clock replacement policy  
 */

class BasicBufferMgr {
   private Buffer[] bufferpool;
   private HashMap<Block,Buffer> bufferPoolMap;
   private int numAvailable;
   private int clockhand=0;
   private int clockcounter= 5;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs, int clockcounter) {
      bufferpool = new Buffer[numbuffs];
      bufferPoolMap=new HashMap<Block,Buffer>();
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer(clockcounter);		//reference counter for clock as args
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
    	 buff = chooseUnpinnedBufferNew();
         if (buff == null)
            return null;
         this.unMapBlock(buff);
         
         buff.assignToBlock(blk);
         this.mapBlock(blk,buff);
         if (!buff.isPinned())
             numAvailable--;
      }
    
      buff.pin();
      if(numAvailable<0)
    	  System.out.println("Buffers available:0");
      else
    	  System.out.println("Buffers available:" + numAvailable); 
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(!buff1.isPinned())
			  System.out.print("- ");
		  else {
			  if(buff1.block() != null)
				  System.out.print(buff1.block().number() + " " );
			  else
				  System.out.print("Null" );
		  }
	  }
	  System.out.println();
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(buff1.block() != null)
			  System.out.print(buff1.block() + " pincount:" + buff1.getPins() + ",");
		  else
			  System.out.print("Null" + " pincount:-"  + ",");
		  if(buff1.getRefbit()==-1)
			  System.out.println("Reference count:-");
		  else
			  System.out.println("Reference count:" + buff1.getRefbit());
	  }

      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {	   
	  Buffer buff = chooseUnpinnedBufferNew();
      if (buff == null) 
         return null;
      this.unMapBlock(buff);
      buff.assignToNew(filename, fmtr);
      this.mapBlock(buff.block(),buff);
      numAvailable--;
      buff.pin();
      if(numAvailable<0)
    	  System.out.println("Buffers available:0");
      else
    	  System.out.println("Buffers available:" + numAvailable); 
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(!buff1.isPinned())
			  System.out.print("- ");
		  else {
			  if(buff1.block() != null)
				  System.out.print(buff1.block().number() + " " );
			  else
				  System.out.print("Null" );
		  }
	  }
	  System.out.println();
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(buff1.block() != null)
			  System.out.print(buff1.block() + " pincount:" + buff1.getPins() + ",");
		  else
			  System.out.print("Null" + " pincount:-"  + ",");
		  if(buff1.getRefbit()==-1)
			  System.out.println("Reference count:-");
		  else
			  System.out.println("Reference count:" + buff1.getRefbit());
	  }
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned() && buff.getRefbit()==0)
         numAvailable++;
      if(numAvailable<0)
    	  System.out.println("Buffers available:0");
      else
    	  System.out.println("Buffers available:" + numAvailable); 
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(!buff1.isPinned())
			  System.out.print("- ");
		  else {
			  if(buff1.block() != null)
				  System.out.print(buff1.block().number() + " " );
			  else
				  System.out.print("Null" );
		  }
	  }
	  System.out.println();
	  for(int i=0,j=i+1;i<bufferpool.length;i++,j++){
		  Buffer buff1 = bufferpool[i];
		  if(buff1.block() != null)
			  System.out.print(buff1.block() + " pincount:" + buff1.getPins() + ",");
		  else
			  System.out.print("Null" + " pincount:-"  + ",");
		  if(buff1.getRefbit()==-1)
			  System.out.println("Reference count:-");
		  else
			  System.out.println("Reference count:" + buff1.getRefbit());
	  }

   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
	Buffer buffer = this.bufferPoolMap.get(blk);
	if(buffer!=null)
  	{
	   return buffer;
  	}
      return null;
   }

   
   private void mapBlock(Block block,Buffer buffer) {
       this.bufferPoolMap.put(block,buffer);
   }
   
   private void unMapBlock(Buffer buffer)
   {
	   if(buffer != null) {
           this.bufferPoolMap.remove(buffer.block());
	   }
   }
   
   /**  
   * Determines whether the map has a mapping from  
   * the block to some buffer.  
   * @param blk the block to use as a key  
   * @return true if there is a mapping; false otherwise  
   */  
   boolean containsMapping(Block blk) {  
	   return bufferPoolMap.containsKey(blk);  
   } 
   
   /**  
   * Returns the buffer that the map maps the specified block to.  
   * @param blk the block to use as a key  
   * @return the buffer mapped to if there is a mapping; null otherwise  
   */  
   Buffer getMapping(Block blk) {  
	   return bufferPoolMap.get(blk);  
   } 
   
   /**
    * Returns buffer whose pin is zero and 
    * reference counter is also zero.
    * If no such buffer found in clockcounter runs 
    * returns null
   **/
   
   private Buffer chooseUnpinnedBufferNew() {
	   int startpos=clockhand;
	   for(int i=0;i<=clockcounter;i++){
		   int count=0;
		   for (int j=startpos; count!= bufferpool.length; count++,j=(++j) %(bufferpool.length)){
			 Buffer buff=bufferpool[j];
	         if (!buff.isPinned()){
	        	 if(!buff.isRefbit()){
	        		 if(numAvailable <= 0)
	        			 System.out.println("GClock policy used, Block replaced:" + buff.block());
	        		 clockhand=(j+1) %(bufferpool.length);
	        		 return buff;
	        	 }
	         }
	   	  }
	   }
	   return null;
   }
}
