package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.server.SimpleDB;

import java.util.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;
   private PageFormatter fmtr = null;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    */
   public void commit() {
	  System.out.println("Inside Commit In Class Recovery Mgmr");
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then writes a quiescent checkpoint record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);

   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      int newblknum = 0; 
      Block blk = buff.block();
      Block newblk = buff.block();
      newblk = buff.saveBlock(blk);
      System.out.println(newblk+"after save block call");
      if (newblk!= null)
      {	  
	      int saveBlknum = newblk.number();
	      newblknum = saveBlknum;
      }
      if (isTempBlock(blk))
         return -1;
      else if (newblk!= null) 
      {
    	  return new UpdateRecord(txnum, blk, offset, oldval, newblknum).writeToLog();
      }
      else return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      int newblknum = 0;
      Block blk = buff.block();
      Block newblk = buff.block();
      newblk = buff.saveBlock(blk);
      System.out.println(newblk+"after save block call");
      if (newblk!= null)
      {
	     int saveBlknum = newblk.number();
	     newblknum = saveBlknum;
      }
      if (isTempBlock(blk))
         return -1;
      else if (newblk!= null)
      {
    	  return new UpdateRecord(txnum, blk, offset, oldval, newblknum).writeToLog();
      }
      else return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * The method stops when it encounters a CHECKPOINT record
    * or the end of the log.
    */
   private void doRecover() {
      Collection<Integer> finishedTxs = new ArrayList<Integer>();
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.op() == CHECKPOINT)
            return;
         if (rec.op() == COMMIT || rec.op() == ROLLBACK)
            finishedTxs.add(rec.txNumber());
         else if (!finishedTxs.contains(rec.txNumber()))
        	rec.undo(txnum);
      }
   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
