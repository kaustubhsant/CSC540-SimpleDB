package simpledb.tx.recovery;

import simpledb.server.SimpleDB;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;

class UpdateRecord implements LogRecord {
   private int txnum;
   private int newblknum = 0;
   private Block blk;   
   
   /**
    * Creates a new update log record.
    * @param txnum the ID of the specified transaction
    * @param blk the block containing the value
    * @param offset the offset of the value in the block
    * @param val the new value
    */
   public UpdateRecord(int txnum, Block blk, int newblknum) {
      this.txnum = txnum;
      this.blk = blk;
      this.newblknum = newblknum;
   }
   
   /**
    * Creates a log record by reading five other values from the log.
    * @param rec the basic log record
    */
   public UpdateRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
      String filename = rec.nextString();
      int blknum = rec.nextInt();
      blk = new Block(filename, blknum);
	  newblknum = rec.nextInt();
   }
   
   /** 
    * Writes a update record to the log.
    * This log record contains the UPDATE operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * string value at that offset.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
      Object[] rec = new Object[] {UPDATE, txnum, blk.fileName(),
         blk.number(), newblknum};
      return logMgr.append(rec);
   }
   
   public int op() {
      return UPDATE;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   public String toString() {
      return "<UPDATE " + txnum + " " + blk + " " + newblknum + ">";
   }
   
   /** 
    * Replaces the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls setString to restore the saved value
    * (using a dummy LSN), and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(int txnum) {
      BufferMgr buffMgr = SimpleDB.bufferMgr();
      Buffer buff = buffMgr.pin(blk);
      buff.restoreBlock(txnum, -1);
      buffMgr.unpin(buff);
   }
}
