/*
*  Program to test MySql using "sqljocky" driver on Dart. License: BSD
* 
*  02-Jul-2013   Brian OHara   Commenced initial version developed on Win7.
*
*  16-Jul-2013   Brian OHara   Altered from async terminal-input to sync from
*                              async, so it is therefore now rather verbose
*                              as a result of initially writing for async
*                              console terminal parameterized-input.
*                              
*  18-Jul-2013   Brian OHara   Wrote the minimalist ClassCcy for currency.
*                              
*  24-Jul-2013   Brian OHara   Moved the clear-table to prior to the sleep,
*                 (brianoh)    so it should now be reliable.
*                 
*  31-Jul-2013   Brian OHara   Handle SocketException better, handle read
*                              of fixed-length List from file.
*                                
*  08-Aug-2013   Brian OHara   Create a table "random" to store the list of
*                 (brianoh)    keys to be updated in order to test contention
*                              better. IE. all processes to use same keys for
*                              updates in order to cause more collisions.
*                              
*  08-Aug-2013   Brian OHara   Remove the start-time as a means of achieving
*                 (brianoh)    a near-synchronous state between instances and
*                              use 'control' table instead.
*                              
*  08-Aug-2013   Brian OHara   Added the 'totals' table as a means of comparing
*                 (brianoh)    with the selected totals on completion.
*                 
*  08-Aug-2013   Brian OHara   Introduced some errors in order to test the
*                 (brianoh)    error-handling, and made changes to improve
*                              the error-handling. Needs improving.
*                              
*  08-Aug-2013   Brian OHara   Display of totals and comparison to determine
*                              if inserts and updates balance with table
*                              values.
*  
*  08-Aug-2013   Brian OHara   Put in code for semaphore to detect first
*                              process to start.
*  
*  14-Aug-2013   Brian OHara   Test error-handling.
*  
*  14-Aug-2013   Brian OHara   Create Tables etc.
*  
*  18-Aug-2013   Brian OHara   Added option to use AutoInc or program-
*                              generated id.
*                              
*  18-Aug-2013   Brian OHara   Re-wrote the handling of totals to make it
*                              more streamlined.
*                              
*  20-Aug-2013   Brian OHara   Started testing on Linux 64-bit (Ubuntu 13.04)
*  
*  20-Aug-2013   Brian OHara   Reported problems with Ubuntu 13.04 64-bit on Github.
*  
*  22-Aug-2013   Brian OHara   Reported problems with Ubuntu 13.04 32-bit on Github.
*  
*  24-Aug-2013   Brian OHara   Started testing on Win8. Results slow.
*
*  01-Sep-2013   Brian OHara   Added a rollback in main update exception & some
*                              minor changes re input. Some calls to fRollback()
*                              were not being treated as a Future. There appears to
*                              have been a problem with Rollbacks.
*                              
*  01-Sep-2013   Brian OHara   Added the option of intentional Rollbacks for Inserts
*                              and updates. When used, this will impact times. I will
*                              likely enhance this option at some point.
*                              
*  01-Sep-2013   Brian OHara   Made some changes to error-handling.
*                              
*  01-Sep-2013   Brian OHara   Altered Inserts to specifically test for errors
*                              relating to duplicate-keys.   
*                              I looked at making it possible to use auto-increment
*                              keys at the same time as instances running program-
*                              generated keys. It doesn't appear simple to implement,
*                              using the current methodology of program-generated
*                              keys. MySQL appears to use MAX(ikey) or similar in
*                              order to determine the next auto-increment key value.
*                              So, although auto-increment may be at say "1001", as
*                              soon as a non-autoinc key is added with a key of say 
*                              "10011001", auto-increment will revert to the higher
*                              sequence. While that could possibly be overcome, I
*                              don't plan to spend a lot of time on that. Therefore,
*                              if that situation occurs, the non auto-increment
*                              instances will likely abort because of excessive errors.
*                              
*  01-Sep-2013                 Tidied up program generally and improved comments.
*                               
* ----------------------------------------------------------------------------------
* 
* The purpose of this program is to test the database driver, it is not
* intended as an example of async programming. I've written this in my
* spare time to start learning Dart and to test the driver.
* 
* Please advise any suggestions or problems via "issues". If you see anything
* that could be done better, please let me know. I'm very much still the student.
* 
* Thanks to James for writing the driver and also to the architects and developers
* of Dart and the Dart team and also to Google for creating a great programming
* language and environment.
* 
* One purpose of this program is to enable the ability to put the "system"
* under stress and also stress the database and the driver and create
* collisions in the database in order to test contention. Hopefully, it will
* also help as an example, but as I am still very much a student of Dart,
* any help would be appreciated. I know that much of what has been done
* here can be done better.
* 
* With regard to the number of iterations, a larger number (1000+) likely
* gives a better indication of speed. With regard to the sample size, a
* smaller size likely tests contention better, and a larger sample likely
* gives a better indication of speed. A very-small sample-size will also
* likely not be a good indicator of speed in any situation. With multiple
* instances they will likely be queuing to update, and with a small number
* of instances the data will likely be in cache, but it still may not be
* super-fast with one user and a very small update sample. The number of
* rows in the table will also be significant on timings.
*  
* I have written a virtually identical program to this test program for
* postgresql (xxgreg) which will be loaded onto Github also (when I
* incorporate latest changes).
*                              
* I have tested for handling contention, and that can be confirmed or
* otherwise via the 'totals' table.
*                          
* The layout of tables can be seen in Function fCreateTablesEtc. The
* database requires the following tables (which it creates) :
* 
*    test01    - the main table that is updated
*    sequences - the table for generating Primary Keys
*    control   - table for synchronizing instances of program.
*    random    - the table used for list of keys to be updated.
*    totals    - the table used for storing and balancing totals                                                                                              
*/

import 'dart:async' as async;  // for futures etc.
import 'dart:io';
import 'dart:math';   // for random
import 'package:sqljocky/sqljocky.dart';
////import '../../sqljocky/lib/sqljocky.dart';
import 'package:sqljocky/utils.dart';
////import '../../sqljocky/lib/utils.dart';

const int    I_TOTAL_PROMPTS    = 13;   // the total count of prompts
const int    I_MAX_PROMPT       = 12;   // (data-entry 0 to 10)
const int    I_MAX_PARAM        = 11;    // maximum parameter number (0 to 9)
const int    I_DB_USER          = 0;    // prompt nr
const int    I_DB_PWD           = 1;    // prompt nr
const int    I_DB_NAME          = 2;    // prompt nr
const int    I_MAX_INSERTS      = 3;    // prompt nr
const int    I_INS_ROLLBACKS    = 4;    // prompt nr
const int    I_USE_AUTOINC      = 5;    // prompt nr
const int    I_MAX_UPDATES      = 6;    // prompt nr
const int    I_UPD_ROLLBACKS    = 7;    // prompt nr
const int    I_SELECT_SIZE      = 8;    // prompt nr
const int    I_CLEAR_YN         = 9;    // prompt nr
const int    I_SAVE_YN          = 10;   // prompt nr
const int    I_INSTANCE_TOT     = 11;   // prompt nr
final int    I_CORRECT_YN       = 12;   // prompt nr

const int    I_DEC_PLACES       = 2;    // For Currency
const int    I_TOT_OPEN         = 0;    // for 'totals' table
const int    I_TOT_INSERT       = 1;    // for 'totals' table
const int    I_TOT_UPDATE       = 2;    // for 'totals' table

final String S_DB_HOST              = "localhost";
const String S_CCY_SYMBOL           = "\$";
const String S_CONTROL_KEY          = "1001";   // Key to control table
const String S_DEC_SEP              = ".";  // Decimal separator
const String S_MAIN_TABLE           = "test01";
const String S_PARAMS_FILE_NAME     = "testMy001.txt";
const String S_SEQUENCE_KEY_MAIN    = "1001";   // Key to 'test01' table.
const String S_SEQUENCE_KEY_TOTALS  = "1002";   // key to 'totals' table
const String S_THOU_SEP             = ",";      // Thousands separator

ClassCcy           ogCcy            = new ClassCcy(I_DEC_PLACES);
ClassTerminalInput ogTerminalInput  = new ClassTerminalInput();
ClassFormatCcy     ogFormatCcy      = new ClassFormatCcy(I_DEC_PLACES, S_DEC_SEP,
                                           S_THOU_SEP, S_CCY_SYMBOL);
ClassPrintLine     ogPrintLine      = new ClassPrintLine(true);
ClassRandAmt       ogRandAmt        = new ClassRandAmt(I_DEC_PLACES);
ClassRandNames     ogRandNames      = new ClassRandNames();
ConnectionPool     ogDb;                // Database connection
RawServerSocket    ogSocket;            // used to lock port for 1st instance

void main() {
/*
 * Get User Input
 */
  List<String> lsInput = ogTerminalInput.fGetUserSelections();
    
  bool          tClearMainTable     = (lsInput[I_CLEAR_YN] == "y");
  bool          tFirstInstance;
  bool          tInsertsAborted;
  bool          tUseAutoInc         = (lsInput[I_USE_AUTOINC] == "y");
  ClassTotals   oClassTotals        = new ClassTotals();
  ClassWrapInt  oiClassInt          = new ClassWrapInt();
  ClassWrapList ollClassList        = new ClassWrapList();
  int           iInsertRollbackFreq = int.parse(lsInput[I_INS_ROLLBACKS]);
  int           iInstanceMax        = int.parse(lsInput[I_INSTANCE_TOT]);
  int           iInstanceNr;
  int           iMaxInserts         = int.parse(lsInput[I_MAX_INSERTS]);
  int           iSelectMax          = int.parse(lsInput[I_SELECT_SIZE]);
  int           iUpdateRollbackFreq = int.parse(lsInput[I_UPD_ROLLBACKS]);
  String        sUsername           = lsInput[I_DB_USER];
  String        sPassword           = lsInput[I_DB_PWD];
  String        sDatabase           = lsInput[I_DB_NAME];
 
/*
 * attempt to connect to database
 */
  ogPrintLine.fPrintForce ("Attempting DB Connection");
  try {    
    ogDb = new ConnectionPool(host: S_DB_HOST, port: 3306,
                              user: sUsername, password: sPassword,
                              db: sDatabase);    
  } catch (oError) {
    fFatal(null, "Main", "Unable to connect to MySql");
  }
    
/*
 * Test Connection
 */
  fTestDbConnection()
  .catchError((oError) =>
    fFatal(null, "Main:fTestDbConnection", "Db not connected"))
  .then((tResult) {
    if (tResult != true)
      fFatal(null, "Main:fTestDbConnection", "Database not connected");
/*
 * Test if first instance of program
 */  
    return fCheckIfFirstInstance();
  })
  .catchError((oError) =>
    fFatal(null, "Main:fCheckIfFirstInstance", "TCP Socket test error - ${oError}"))
  .then((tResult) {
    tFirstInstance = tResult;
    ogPrintLine.fPrintForce(tFirstInstance ? 
      "This is the first Instance of program" :
      "This is not the first instance of program");    
    if (!tFirstInstance && iInstanceMax == 1)
      throw("This should be the first instance, however "+
      " there is an instance already running");
    
/*
 * Create tables (controlling (first) instance)
 */
    return !tFirstInstance ? true : fCreateTablesEtc (tClearMainTable);    
  })
  .then((tResult) {
    if (tResult != true)
      fFatal(null, "Main", "Invalid result from fCreateTablesEtc - ${tResult}");
/*
 * Wait for controlling instance to complete initialization. All
 * but the 'controlling' instance wait for the 'controlling' instance
 * to complete initialization.
 */
    return tFirstInstance ? true :
      fWaitForProcess(sColumn: "iCount1", sCompareType: ">",
                      iRequiredValue: 0, sWaitReason: "to initialize",
                      tStart:true); 
  })  
  .then((tResult) {
    if (tResult != true)
      fFatal(null, "Main:fWaitForProcess", "Process did not complete");
/*
 * Update Control table then wait for all instances to start
 * after the 1st instance does this update, the others also do
 */
    int iRequiredCount = tFirstInstance ? 0 : -1;  // 1st instance must be first
    return fUpdateControlTableColumn(sColumn: "iCount1",
                                     iRequiredCount: iRequiredCount,
                                     iMaxCount: iInstanceMax,
                                     oiResult: oiClassInt);
  }).then((bool tResult) {   
    iInstanceNr = oiClassInt.iValue;  // unique instance Nr.
    if (!(iInstanceNr > 0 && iInstanceNr <= iInstanceMax))
      fFatal(null, "Main", "Invalid instance number - ${iInstanceNr}");
    if (tFirstInstance && iInstanceNr != 1)
      fFatal(null, "Main:fUpdateControlTableColumn",
             "This is first instance, however instance nr. = ${iInstanceNr}");
/*
 * wait for All instances to Start
 */
    return fWaitForProcess(sColumn: "iCount1", sCompareType: "=", 
                           iRequiredValue: iInstanceMax,
                           sWaitReason: "to start",
                           tStart: true);
  }).then((bool tResult) {
    ogPrintLine.fPrintForce ("Started processing at ${new DateTime.now()}");
    print ("");

 /*
 * When all instances have started Process Main Inserts
 */ 
    return fProcessMainInserts(iInstanceNr, iMaxInserts, tUseAutoInc,
                               oClassTotals, iInsertRollbackFreq);
  }).then((bool tResult) {
    if (tResult != true && tResult != false)
      fFatal(null, "Main:fProcessMainInserts", "Process failed. Result = (${tResult})");
    tInsertsAborted = tResult;

/*
 * Print and Update totals for Inserts
 */
    oClassTotals.fPrint();
    return fInsertIntoTotalsTable(oClassTotals);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fInsertIntoTotalsTable", "Process failed, Result = ${tResult}");
/*
 * Update 'control' to show inserts have completed
 */
    return fUpdateControlTableColumn(sColumn: "iCount2",
                                     iRequiredCount: -1,
                                     iMaxCount: iInstanceMax,
                                     oiResult: null);

  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fUpdateControlTableColumn", "Process failed. Result = ${tResult}");

/*
 * wait for All processes to complete main Inserts
 */
    return fWaitForProcess(sColumn: "iCount2",
                           sCompareType: "=",
                           iRequiredValue: iInstanceMax,
                           sWaitReason: "to complete Inserts",
                           tStart: false);
 
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fWaitForProcess", "Process failed. Result = ${tResult}");
/*
 * Create List of random keys
 */
    print;
    ogPrintLine.fPrintForce("Insert table of random keys");
    return !tFirstInstance ? true : fInsertRandomKeys(iSelectMax);
  }).then((tResult) {
    if (tResult != true)
      fFatal (null, "Main", "failure to create random keys");
/*
 * first instance only to update control table to show random keys created  
 */
    return !tFirstInstance ? true :    
      fUpdateControlTableColumn(sColumn: "iCount3", 
                                iRequiredCount: 0, 
                                iMaxCount: 1,
                                oiResult: null);
 
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fUpdateControlTable", "Process did not complete");
/*
 * Wait for random key table to be created
 */
    return fWaitForProcess(sColumn: "iCount3", sCompareType: ">",
                            iRequiredValue: 0, sWaitReason:
                            "to propagate random key table", tStart:true);
 
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fWaitForProcess", "(iCount3) Process failed. Result = ${tResult}");
/*
 * Select random table for keys to use for updates
 */
    ogPrintLine.fPrintForce("Selecting random keys from random table");
    print("");
    String sSql = "SELECT ikey FROM random";
    return fProcessSqlSelect(sSql, false, ollClassList);    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fProcessSqlSelect", "(random) Process failed. Result = ${tResult}");
    List<List<int>> lliRandKeys = ollClassList.llValue;
    print;
    int iMaxUpdates = int.parse(lsInput[I_MAX_UPDATES]); // number of insertS
    if ((lliRandKeys == null || lliRandKeys.length == 0) && iMaxUpdates > 0)
      fFatal(null, "On Return from Selecting Random Keys",
       "No List of keys created");
/*
 * Process Main Updates using random keys
 */
    return fProcessMainUpdates(iInstanceNr, iMaxUpdates, lliRandKeys,
                               oClassTotals, iUpdateRollbackFreq);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fProcessMainUpdates", "Process failed. Result = (${tResult})"); 
/*
 * Print and Update totals
 */
    oClassTotals.fPrint();
    return fInsertIntoTotalsTable(oClassTotals);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fInsertIntoTotalsTable", "Process failed. Result = ${tResult}");
/*
 * Select Main Table (unsorted)
 */
    print ("");
    ogPrintLine.fPrintForce ("Processing Select (Unsorted)");
    String sSql = "SELECT * FROM ${S_MAIN_TABLE}";
    return fProcessSqlSelect (sSql, true, null);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fProcessSqlSelect", "(unsorted) Process failed. Result = ${tResult}");
    print("");
/*
 * Select Table (sorted)
 */
    ogPrintLine.fPrintForce ("Processing Select (Sorted)");
    String sSql = "SELECT * FROM ${S_MAIN_TABLE} ORDER BY ikey";
    return fProcessSqlSelect (sSql, true, null);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fProcessSqlSelect", "(sorted) Process failed. Result = ${tResult}");
/*
 * Update control row to increment count of instances finished
 */
    return fUpdateControlTableColumn(sColumn: "iCount4", 
                                     iRequiredCount: -1, 
                                     iMaxCount: iInstanceMax,
                                     oiResult: null);  
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fUpdateControlTableColumn", "(iCount4) Process failed. Result = ${tResult}");      
/*
 * wait for All processes to Finish
 */
    print ("");
    return fWaitForProcess(sColumn: "iCount4",
                           sCompareType: "=",
                           iRequiredValue: iInstanceMax,
                           sWaitReason: "to complete Processing",
                           tStart: false);
    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal(null, "Main:fWaitForProcess", "(iCount4) Process failed. Result = ${tResult}");
/*
 * Display and compare totals to ensure they balance.
 */
    print ("");
    return fDisplayTotals();
  }).then((tResult) {
    if (tResult != true)
      fFatal(null, "Main:fDisplayTotals", "Failed to complete");
    ogPrintLine.fPrintForce ("Completed");
    if (tInsertsAborted) {
      print ("**** Note: Inserting of Rows was aborted! ****");
    }
    fExit(0);
  })
  .catchError((oError) =>
    fFatal(null, "Main", "${oError}"));
}

/*
 ***** Process Inserts To Main Table ****
 */
async.Future<bool> fProcessMainInserts(int iInstanceNr, int iMaxUpdates,
                                      bool tUseAutoInc, ClassTotals oClassTotals,
                                      int iRollbackFreq) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  
  bool          tAbort           = false; 
  ClassWrapList ollClassList     = new ClassWrapList();
  ClassWrapString osClassString  = new ClassWrapString();
  int           iCcyInsertTot    = 0;
  int           iDiv             = fGetDivisor(iMaxUpdates);  // for progress indic.
  int           iDupKeyTot       = 0;
  int           iInsertTotNr     = 0;
  int           iInsertFailTot   = 0;
  int           iInsertRollbacks = 0;
  int           iLastIdTot       = 0;
  int           iLastLog         = 0;
  Function      fLoopInserts;
  Stopwatch oStopwatch = new Stopwatch();
  
  fLoopInserts = () {
    if (iInsertTotNr % iDiv == 0 && iInsertTotNr != iLastLog) {
      stdout.write ("${iInsertTotNr}  ");
      iLastLog = iInsertTotNr;
    }
    if (iInsertFailTot > 100 && iInsertFailTot > (iInsertTotNr + iInsertRollbacks)) {
      print ("**** fProcessMainInserts: ${iInsertFailTot} Inserts have failed, "+
             "${iInsertTotNr} have succeeded ****");
      print ("Aborting Inserts");
      tAbort = true;
    }

    if (iInsertTotNr >= iMaxUpdates || tAbort) {   // all done
      oStopwatch.stop;
      print("");
      
      if (iInsertFailTot - iDupKeyTot > 0) {
        ogPrintLine.fPrintForce("Failed Inserts (system stress?) = "+
                                "${iInsertFailTot}");
      } else {
        ogPrintLine.fPrintForce("Failed Inserts = "+
            "${iInsertFailTot}");
      }
      ogPrintLine.fPrintForce("Duplicate keys encountered = ${iDupKeyTot}");
      if (iRollbackFreq > 0 || iInsertRollbacks > 0)
        ogPrintLine.fPrintForce("Intentional Rollbacks = ${iInsertRollbacks}");
      print("");
      ogPrintLine.fPrintForce ("total valid Last Id's = ${iLastIdTot}");
      
      oClassTotals.fSetValues (iInstance    : iInstanceNr,
                               iTotType     : I_TOT_INSERT,
                               iInsertTotNr : iInsertTotNr,
                               iUpdateTotNr : 0,
                               iCcyTotAmt   : iCcyInsertTot,
                               iTotMillis   : oStopwatch.elapsedMilliseconds);
      
      oCompleter.complete(tAbort);
      return;
    }
    
    String sName    = ogRandNames.fGetRandName();
    int    iCcyBal  = ogRandAmt.fRandAmt(99999, (iInsertTotNr % 2 == 0));
    String sCcyBal  = ogCcy.fCcyIntToString(iCcyBal);

/*
 * Force a Rollback if selected
 */  
    bool tForceRollback = (iRollbackFreq > 0 && (iInsertRollbacks < 
                           ((iInsertTotNr +1) / iRollbackFreq).toInt()));
    if (tForceRollback) {
      sCcyBal = "1234.aa";
    }
/*
 * Don't use AutoInc
 */
    if (!(tUseAutoInc)) {
      String sSql = "(iKey, sname, dbalance)"+
                     " VALUES (?, '$sName', $sCcyBal)"; 

      fInsertRowWithSequence(S_MAIN_TABLE, S_SEQUENCE_KEY_MAIN, sSql,
                             tForceRollback, osClassString)
      .then((tResult) {
        if (tResult == true) {
          iInsertTotNr++;
          iCcyInsertTot += iCcyBal;
        } else if (tResult == false) {
          throw("non-autoinc Insert failed");
        } else {
          fFatal(null, "fProcessMainInserts", 
              "Invalid response from fExecuteSql - ${tResult}");
        }
        fLoopInserts();
      })
      .catchError((oError) {
        if (tForceRollback) {
          iInsertRollbacks++;
        } else {
          iInsertFailTot++;
          if (osClassString.sValue.contains("Error 1062")) {   // duplicate key
            iDupKeyTot++;            
          }
          print ("fProcessMainInserts: Insert failed - ${oError}");
        }
        fLoopInserts();
      });
/*
 * Do use AutoInc
 */
    } else {
      String sSql = "INSERT INTO ${S_MAIN_TABLE} (sname, dbalance)"+
                     " VALUES ('$sName', $sCcyBal)";

      fExecuteSql(sSql, S_MAIN_TABLE, "fProcessMainInserts", 1, false)

      .then((bool tResult) {
        if (!([true,false].contains(tResult)))
          fFatal(null, "fProcessMainInserts", 
                  "Invalid response from fExecuteSql - ${tResult}");
        if (!(tResult)) {
          if (tForceRollback) {
            iInsertRollbacks++;
          } else {
            iInsertFailTot++;
            print ("Insert failed");
          }
          fLoopInserts();
        } else {
          iInsertTotNr++;
          iCcyInsertTot += iCcyBal;
          fProcessSqlSelect("SELECT LAST_INSERT_ID()", false, ollClassList)
          .then((bool tResult){
            if (tResult != true)
              fFatal(null, "fProcessMainInserts:fProcessSqlSelect",
                            "Process failed. Result = ${tResult}");
            List<List<int>> lliResult = ollClassList.llValue;
            if(lliResult != null && lliResult.length == 1) {
              int iKey = lliResult[0][0];
              if (iKey > 0)
                iLastIdTot++;
            }
            fLoopInserts();
          });
        }
      });
    }
  };  
  ogPrintLine.fWriteForce ("Processing Inserts .... ");
  oStopwatch.start();
  fLoopInserts();
  return oCompleter.future;
}  

/*
 **** Insert a Single Row into a table using program-generated sequence ****
 **** number. 'sequences' table holds last number used for Key.         ****
 */
async.Future<bool> fInsertRowWithSequence (String sTableName,
                          String sSequenceKey, String sSql1,
                          bool tForceRollback, ClassWrapString osClassString) {

  async.Completer<bool> oCompleter = new async.Completer<bool>();  

/////  fCheckTransactionStatus("fInsertRowWithSequence", true);
  if (osClassString != null) {
    osClassString.sValue = "";   // init
  }
  var oTxn;
  String sNewKey;   // the new key to be inserted
  ogDb.startTransaction()
  .then((oResult) {
    oTxn = oResult;
    String sSql2 = "SELECT iLastkey FROM sequences WHERE ikey = "+
                   "${sSequenceKey} FOR UPDATE";
    return oTxn.query(sSql2)
    .then((oResult){
      return oResult.stream.toList();
    })
    .then((llRows) {               
      if (llRows == null || llRows.length != 1)                 
        throw ("fInsertRowWithSequence: row '${sSequenceKey}' "+
                "for table ${sTableName} is missing from "+
                "'sequences' table");
      sNewKey = (llRows[0][0] +1).toString();

      sSql1 = "INSERT INTO " +sTableName +(sSql1.replaceFirst(
          "?", sNewKey));
 
      return oTxn.query(sSql1);
    })
    .then((oResult) {
      if (oResult.affectedRows != 1)
        throw ("Insert ${sTableName}: New Key = ${sNewKey}, "+
                "rows affected Not 1 but = "+
                "${oResult.affectedRows}");
      return oTxn.query("UPDATE sequences SET ilastkey = "+
                         "${sNewKey} WHERE ikey = ${sSequenceKey}");      
    })
    .then((oResult) {
      if (oResult.affectedRows != 1)
        throw ("Table ${sTableName}, Update "+
                "Sequences: Rows Not 1 but = ${oResult.affectedRows}");
      
      oTxn.commit()
      .then((_) {
        oCompleter.complete(true);
        return;
////      fCheckTransactionStatus("fInsertRowWithSequence - after COMMIT", false);
////      return;
      });
    });
  })
  .catchError((oError) {
    if (osClassString != null) {
      osClassString.sValue = "${oError}";
    }
    if (!(tForceRollback)) {
      print ("fInsertRowWithSequence: Table ${sTableName} - Insert failed. "+
              "Key = ${sNewKey}, \nError = ${oError}");
    }
    fRollback(oTxn).then((tResult){
      oCompleter.complete(false);
    });
  });
  
  return oCompleter.future;
}

/*
 ***** Process Updates for Main Table using list of keys ****
 */
async.Future<bool> fProcessMainUpdates(int iInstanceNr, int iMaxUpdates, List lRandKeys,
                                       ClassTotals oClassTotals, int iRollbackFreq) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  
  Function  fLoop;
  int       iCcyUpdateTot    = 0;
  int       iDiv             = fGetDivisor(iMaxUpdates);  // Divisor for progress indicator
  int       iUpdateTotNr     = 0;
  int       iUpdateFailTot   = 0;
  int       iUpdateRollbacks = 0;
  Random    oRandom          = new Random();
  Stopwatch oStopwatch       = new Stopwatch();
  
  if ((lRandKeys == null || lRandKeys.length < 1) && iMaxUpdates > 0)
    fFatal (null, "fProcessUpdates",
     "List of random keys not was not created");
 
  ogPrintLine.fWriteForce ("Processing Updates .... ");

  oStopwatch.start();

  fLoop = () {
    if (iUpdateTotNr >= iMaxUpdates) {   // then all completed
      oStopwatch.stop;
      print("");
      ogPrintLine.fPrintForce("Failed Updates (system stress or contention) "+
                               "= ${iUpdateFailTot}");
      
      if (iRollbackFreq > 0 || iUpdateRollbacks > 0)
        ogPrintLine.fPrintForce("Intentional Rollbacks = ${iUpdateRollbacks}");      
      
      print("");
      
      oClassTotals.fSetValues (iInstance    :  iInstanceNr,
                               iTotType     : I_TOT_UPDATE,
                               iInsertTotNr : 0,
                               iUpdateTotNr : iUpdateTotNr,
                               iCcyTotAmt   : iCcyUpdateTot,
                               iTotMillis   : oStopwatch.elapsedMilliseconds);
      
      oCompleter.complete(true);
      return;
    }
    int iKey = lRandKeys[oRandom.nextInt(lRandKeys.length)][0];
    int iCcyTranAmt = ogRandAmt.fRandAmt(999, (iUpdateTotNr % 2 == 0));

/*
 * Force a Rollback if required
 */
    bool tForceRollback = (iRollbackFreq > 0 && (iUpdateRollbacks < 
                           ((iUpdateTotNr+1) / iRollbackFreq).toInt()));
    
    fUpdateMainRow(iKey, iCcyTranAmt, tForceRollback)
    .then((tResult) {
      if (tResult) {
        if (tForceRollback) {
          fFatal(null, "fProcessMainUpdates", "Update Rollback forced, "+
          "but update succeeded");
        }
        iCcyUpdateTot += iCcyTranAmt;
        if (++iUpdateTotNr % iDiv == 0) {
          stdout.write("${iUpdateTotNr}   ");        
        }
      } else {
        if (tForceRollback) {
          iUpdateRollbacks++;
        } else {
          iUpdateFailTot++;
          print ("Update failed");
        }
        if (iUpdateFailTot > 100 &&
            iUpdateFailTot > iUpdateTotNr) {
          fFatal(null, "fProcessMainUpdates", "${iUpdateFailTot} "+
                       "updates have failed, and only "+
                       "${iUpdateTotNr} have succeeded");
        }
      }
      fLoop();
    });
  };
  fLoop(); 
  return oCompleter.future;
}  

/*
 ***** Update a Single Row of Main Table ****
 */
async.Future<bool> fUpdateMainRow(int iKey, int iCcyTranAmt, bool tForceRollback) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  var oTxn;
  String sCcyTranAmt  = ogCcy.fCcyIntToString(iCcyTranAmt);
  
  ogDb.startTransaction()
  .then((oTransaction) {
    oTxn = oTransaction;
    String sSql = "SELECT ikey, dbalance FROM ${S_MAIN_TABLE} WHERE iKey = "+
     "${iKey} FOR UPDATE";
    return oTxn.query(sSql)
    .then((oResult){
      return oResult.stream.toList();
    })
    .then((List<List> llRows) {
      if (llRows == null || llRows.length != 1)
        fFatal(oTxn, "Update Data", "Select failed. Rows = ${llRows}");
      String sKey         = "${llRows[0][0]}";       
      double dBalOld      = llRows[0][1];
      String sCcyTranAmt  = ogCcy.fCcyIntToString(iCcyTranAmt);
      double dBalNew      = ogCcy.fAddCcyDoubles(dBalOld,
                                                 double.parse(sCcyTranAmt));
      String sBalNew      = dBalNew.toStringAsFixed(I_DEC_PLACES);
      if (tForceRollback)   // cause a Rollback
        sBalNew = "1234.aa";
      String sSql         = "UPDATE test01 SET dBalance = $sBalNew " +
                            "WHERE ikey = $sKey";
      return oTxn.query(sSql);
    })
    .then((oResult) {
      if (oResult.affectedRows == 1) {
        if (tForceRollback) {
          fFatal (null, "fUpdateMainRow", "Rollback being forced, "+
                  "however update succeeded");
        }
        oTxn.commit()
        .then((_) =>
          oCompleter.complete(true));
      } else {  
        print ("Update failed - rows affected = ${oResult.affectedRows}");
        print ("Sql = "+sSql);
        fRollback(oTxn).then((tResult){
          oCompleter.complete(false);
          return;
        });
      }
    });
  })
  .catchError((oError) {
    if (!(tForceRollback)) {
      print ("fUpdateMainRow: Update failed - ${oError}");
    }
    fRollback(oTxn).then((tResult){
      oCompleter.complete(false);
    });
  });
 
  return oCompleter.future; 
}
      
/*
 **** Select a Table ****
 */
async.Future<bool> fProcessSqlSelect (String sSql, bool tShowTime,
                                      ClassWrapList ollResult) {
  Stopwatch oStopwatch = new Stopwatch();
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  oStopwatch.start();
  ogDb.query(sSql)
  .then((oResult){
    return oResult.stream.toList();
  })
  .then((llRows) {
    if (ollResult != null)
      ollResult.llValue = llRows;
    oStopwatch.stop();
    if (llRows.length < 1)
      ogPrintLine.fPrintForce ("Select failed to return any rows");
    int iTotRows = llRows.length;
    int iMillis = oStopwatch.elapsedMilliseconds;
    String sElapsed = (iMillis/1000).toStringAsFixed(2);
    if (tShowTime) {
      ogPrintLine.fPrintForce ("Total rows selected = $iTotRows, "+
       "elapsed = $sElapsed seconds");
      
      if (iTotRows > 0) {
        sElapsed = (iMillis/iTotRows).toStringAsFixed(4);
    
        ogPrintLine.fPrintForce ("Average elapsed milliseconds = "+
         sElapsed);
      }
    }
    oCompleter.complete(true);
    return;
  }).catchError((oError) =>
    fFatal(null, "fProcessSqlSelect", "${oError}"));

  return oCompleter.future;  
}

/*
 **** Rollback Transaction ****
 */
async.Future<bool> fRollback(Transaction oTxn) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  if (oTxn == null) {
    oCompleter.complete(true);
  } else { 
////    print ("Attempting Rollback");
    oTxn.rollback().then((_) {
      oCompleter.complete(true);
    }).catchError((oError) {
      print ("Rollback failed. Error = ${oError}");
      oCompleter.complete(false);
    });
  }
  return oCompleter.future;
}

/*
 **** Connect To Database ****
 */
async.Future<bool> fConnectToDb(String sDbHost, String sUsername,
                                String sPassword, String sDatabase) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  try {
    ogDb = new ConnectionPool(
            host: sDbHost, port: 3306, user: sUsername,
            password: sPassword, db: sDatabase);

    ogPrintLine.fPrintForce ("Database Connected");
    oCompleter.complete((ogDb != null)); 
    return oCompleter.future;
  } catch(oError) {
    String sErrorMsg = (oError is SocketException) ?
                        "Database is not connected" :
                        "Fatal error encountered ${oError}";
    fFatal(null, "fConnectToDb", sErrorMsg);
  }
}

/*
 **** Test Db Connection ****
 */
async.Future<bool> fTestDbConnection() {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  
  ogPrintLine.fPrintForce ("Testing Db Connection");

  String sSql = "DROP TABLE IF EXISTS testmy001";
  
  fExecuteSql(sSql, "testmy001", "fTestDbConnection", -1, false)
  .catchError((oError) =>
    oCompleter.complete(false))
  .then((tResult) =>
    oCompleter.complete(tResult));
  
  return oCompleter.future;
}  

/*
 **** Insert the List of random keys into 'random' table for selection ****
 */
async.Future<bool> fInsertRandomKeys(int iSelectMax) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  ClassWrapList ollClassList        = new ClassWrapList();
  Function      fLoopRandomInserts;
  int           iPos                = 0;
  List<List<int>> lliKeys;
  
  fLoopRandomInserts = () {
    if (iPos >= lliKeys.length)
      return;
    
    int iKey = lliKeys[iPos][0];
    String sSql = "INSERT INTO random (iKey) VALUES (${iKey})";
    return fExecuteSql(sSql, "random", "fInsertRandomKeys", 1, true)
    .then((bool tResult){
      if (tResult == true)
        iPos++;
      fLoopRandomInserts();
    })
    .catchError((oError) =>
      throw ("fInsertRandomKeys: (${iPos}) ${oError}"));
  };
  
  String sSql = "SELECT ikey FROM ${S_MAIN_TABLE} ORDER BY RAND() " +
                 "LIMIT ${iSelectMax}";  

  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fInsertRandomKeys", "Process failed. Result = ${tResult}");
    lliKeys = ollClassList.llValue;  
    if (lliKeys == null)
      fFatal (null, "fInsertRandomKeys", "Select of keys from "+
              "${S_MAIN_TABLE} failed to select any keys");

    ogPrintLine.fPrintForce("${lliKeys.length} random rows selected");
    fLoopRandomInserts();
  
    oCompleter.complete(true);
    
  })
  .catchError((oError) =>
    fFatal(null, "fInsertRandomKeys", "${oError}"));
  
  return oCompleter.future;
}

/*
 **** Fatal Error Encountered ****
 */
void fFatal(Transaction oTxn, String sCheckpoint, String sError) {
  print ("Fatal error. $sCheckpoint. Error = $sError");
  fRollback(oTxn).then ((tResult){
    print ("Program terminated");
    fExit(1);
  });
}

/*
 **** Update Control Table with counter to handle synchronization. ****
 */
async.Future<bool> fUpdateControlTableColumn({String sColumn,
                                            int iRequiredCount,
                                            int iMaxCount,
                                            ClassWrapInt oiResult}) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
////  fCheckTransactionStatus("fUpdateControlTableColumn", true);
  
  int    iCount;
  String sSql = "SELECT ${sColumn} FROM control WHERE "+
                 "ikey = $S_CONTROL_KEY FOR UPDATE";
  var oTxn;

  ogDb.startTransaction()
  .then((oResult) {
    oTxn = oResult;
    return oTxn.query(sSql)
    .then((oResult){
      return oResult.stream.toList();
    })
    .then((llRows) {
      if (llRows == null || llRows.length != 1)
        fFatal(oTxn, "fUpdateControlTableColumn",
                "Select failed. Rows = ${llRows}");
 
      iCount = llRows[0][0];
      if (iCount >= iMaxCount)
      throw ("fUpdateControlTableColumn: Fatal Error - Maximum "+
              "count = ${iMaxCount}, current count = ${iCount}");
      
      if (iRequiredCount >= 0 && iCount != iRequiredCount)
        throw ("fUpdateControlTableColumn: Required count = ${iRequiredCount}, "+
                "Current count = ${iCount}");
    
      sSql = "UPDATE control SET ${sColumn} = ${++iCount} "+
              "WHERE ikey = $S_CONTROL_KEY";

      return oTxn.query(sSql);
    })
    .then((oResult) {
      if (oResult.affectedRows != 1)
        throw ("fUpdateControlTableColumn: Rows affected = "+
                "${oResult.affectedRows}, should = 1");
      
      return oTxn.commit();
    })
    .then((_){
      if (oiResult != null)   // then result is wanted
        oiResult.iValue = iCount;
      oCompleter.complete(true);
      return;
    });
  })
  .catchError((oError) =>
    fFatal(oTxn, "fUpdateControlTableColumn",
          "${oError} - Sql = $sSql"));

  return oCompleter.future; 
}

/*
* Wait for other instances to complete. The "control" table
* handles synchronization by updating counters and then 
* waiting for a required number (of processes) to have completed
* their update.
*/
async.Future<bool> fWaitForProcess({String sColumn,
                                   String sCompareType,
                                   int iRequiredValue,
                                   String sWaitReason,
                                   bool tStart}) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  ClassWrapList ollClassList     = new ClassWrapList(); 
  Function      fWaitLoop;
  int           iLastCount;
  int           iLoopCount = 0;
  
  fWaitLoop = () {
/*
 * The sleep is only one second in order that there is not an
 * excessive wait after all are "ready". This could be handled
 * better (using this methodology), but it would be a little more
 * complex. I'll likely change this.
 */
    new async.Timer(new Duration(seconds:1), () {
      String sSql = "Select ${sColumn} from control where ikey = "+
                     "$S_CONTROL_KEY";
      fProcessSqlSelect(sSql, false, ollClassList)
      .then((bool tResult) {
        if (tResult != true)
          fFatal(null, "fWaitForProcess",
                 "(${sColumn}) - Select failed. Result = ${tResult}");
        List<List<int>> llRow = ollClassList.llValue; 
        bool tMatched = false;   // init
        if (llRow == null || llRow.length != 1)
          fFatal(null, "fWaitForProcess", "Failed to sSelect control row");
        int iCount = llRow[0][0];
        if (iCount != iLastCount) {
          iLastCount = iCount;
          String sInstances = iCount == 1 ? "instance has" :"instances have";
          String sPline = "${iCount} ${sInstances}";
          sPline += tStart ? " started." : " completed.";
          
          tMatched = ((sCompareType == "=" && iCount == iRequiredValue)
                      || (sCompareType == ">" && iCount > iRequiredValue));
          if (!tMatched) {
            sPline += " Waiting for $sCompareType ${iRequiredValue} instances ";
           ///// sPline += tStart ? "start " : "complete ";
            ogPrintLine.fPrintForce(sPline + "${sWaitReason}.");          
          }
          if (!tMatched && iCount > iRequiredValue)          
            throw ("instance count exceeded");
        }
        if (tMatched) {  // all processes are ready to go
          ogPrintLine.fPrintForce("Wait completed for ${iCount} instance(s) "
                                   +sWaitReason);
          oCompleter.complete(true);
          return;
        }
        iLoopCount++;
        fWaitLoop();
      })
      .catchError((oError) =>
        fFatal (null, "fWaitForProcess", "${oError}"));
    });
  };
  fWaitLoop();
  
  return oCompleter.future;
}

/*
 * Get Divisor For Number of Iterations - To Display Progress
 */
int fGetDivisor(int iMaxIters) =>
    iMaxIters >= 100000 ? 20000 :
    iMaxIters >   20000 ?  5000 :
    iMaxIters >    7000 ?  2000 :
    iMaxIters >    3000 ?  1000 :
    iMaxIters >    1000 ?   500 :
                            100;

/*
 * Get a Random Amount For Update or Insert
 */
class ClassRandAmt {
  Random _oRandom; 
  int _iDecPlaces;
  int _iScale = 1;

  ClassRandAmt(int iDecPlaces) {
    _iDecPlaces = iDecPlaces;
    _oRandom   = new Random();
    for (int i=0; i < iDecPlaces; i++, _iScale *= 10);   // set scale for cents
  }
  
  int fRandAmt(int iMaxDollars, bool tPositive) {
    int iCcyAmt = _oRandom.nextInt((iMaxDollars * _iScale) + (_iScale-1))+1;
    if (! tPositive)    // then make negative
      iCcyAmt = -iCcyAmt;
    return iCcyAmt;
  }
}

/*
 * Display on completion the totals from the
 * main table and also the 'totals' table 
 */
async.Future<bool> fDisplayTotals() {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
  ClassWrapList ollClassList  = new ClassWrapList();
  double        dCcyTot1      = 0.00;    // init
  int           iRowTot1      = 0;       // init
  
  String sSql = "SELECT count(*), sum(dbalance) FROM ${S_MAIN_TABLE}";
  
  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fDisplayTotals",
             "Select of ${S_MAIN_TABLE} failed. Result = ${tResult}");
/*
 * Display totals from select of main table
 */
    List<List> llRows = ollClassList.llValue;
    if (llRows == null || llRows.length < 1)
      fFatal(null, "fDisplayTotals",
             "Select of '${S_MAIN_TABLE}' table failed to return a row");
      
    if (llRows[0][0] != null)
      iRowTot1 = llRows[0][0];
    if (llRows[0][1] != null)
      dCcyTot1 = llRows[0][1];
    
    String sCcyTot1 = ogFormatCcy.fFormatCcy(dCcyTot1);
    ogPrintLine.fPrintForce
    ("Current '${S_MAIN_TABLE}' table totals: Rows = ${iRowTot1}, "+
       "Balances = ${sCcyTot1}");
    
    return true;
  })
  .then((_) {   
  
/*
 *  Display totals from select of totals table
 */    
    int    iRowTot2  = 0;       // init
    double dCcyTot2  = 0.0;     // init

    sSql = "SELECT sum(iInsertNr), sum(dccyvalue) FROM totals";
    fProcessSqlSelect(sSql, false, ollClassList)
    .then((bool tResult) {
      if (tResult != true)
        fFatal(null, "fDisplayTotals",
               "Select of 'totals' table failed. Result = ${tResult}");
      List<List> llRows = ollClassList.llValue;
      if (llRows == null || llRows.length < 1)
        print ("fDisplayTotals: Select of 'totals' table failed to return a row");
      else {
        if (llRows[0][0] != null) {
          double dRowTot2 = llRows[0][0]; // sum() returning double 
          iRowTot2 = dRowTot2.toInt();
        }
        if (llRows[0][1] != null)
          dCcyTot2 = llRows[0][1];      
      }
      String sCcyTot2 = ogFormatCcy.fFormatCcy(dCcyTot2);
      ogPrintLine.fPrintForce("Current 'totals' table totals: Rows = "+
                               "${iRowTot2}, Balances = ${sCcyTot2}");
    
      double dCcyDiff = ogCcy.fAddCcyDoubles(dCcyTot1, -dCcyTot2);
      int    iRowsDiff = iRowTot1 - iRowTot2;
      
      if (iRowsDiff == 0 && dCcyDiff == 0.00)
        ogPrintLine.fPrintForce("**** Totals DO Balance ****");
      else {
        String sCcyDiffFormatted = ogFormatCcy.fFormatCcy(dCcyDiff); 
        ogPrintLine.fPrintForce("!!!! Totals DO NOT balance !!!!");
        ogPrintLine.fPrintForce (
            "Difference in Rows = ${iRowsDiff} "+
            "Difference in Value = ${sCcyDiffFormatted}");
      }
      oCompleter.complete(true);
      return;
    });
  })
  .catchError((oError) =>
    fFatal (null, "fDisplayTotals", "${oError}"));
  
  return oCompleter.future;
}

/*
 ***** create if necessary the tables and data used by this program. ****
 */
async.Future<bool> fCreateTablesEtc (bool tClearMainTable) { 
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  const int I_MIN_KEY_MAIN = 10011000;
  
  ClassWrapList ollClassList  = new ClassWrapList();
  
/*
 * Create 'control' Table
 */
  String sSql = "CREATE TABLE IF NOT EXISTS control "+
      "(iKey int Primary Key not null unique, "+
      "icount1 int not null, icount2 int not null, "+
      "icount3 int not null, icount4 int not null, "+
      "icount5 int not null, icount6 int not null)";
  
  fExecuteSql(sSql, "control", "fCreateTables", -1, true)
  .catchError((oError) =>
    throw("Create 'control' - ${oError}"))
  .then((bool tResult) {   
    if (tResult != true)
      fFatal(null, "fCreateTablesEtc", "failed to create 'control' table");
  
/*
 * Delete row from 'control' table (later inserted)
 */
    sSql = "DELETE FROM control WHERE ikey = ${S_CONTROL_KEY}";
  
    return fExecuteSql (sSql, "control", "fCreateTables", -1, true);
  })
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fCreateTablesEtc", "failed to delete 'control' row");
/*
 * Insert required row into 'control' table
 */
    sSql = "INSERT INTO control (ikey, icount1, icount2, "+
            "icount3, icount4, icount5, icount6) "+
            "VALUES ($S_CONTROL_KEY,0,0,0,0,0,0)";
  
    return fExecuteSql(sSql, "control", "fInsertSingleControlRow", 1, true);
  })
  .catchError((oError) =>
    throw("Insert into 'control'"))
  .then((bool tResult){
    if (tResult != true)
      fFatal(null, "fCreateTables", "Unable to Insert into 'control'");
/*
 *  Create Main table used for Inserts and Updates
 */
    sSql = "CREATE TABLE IF NOT EXISTS ${S_MAIN_TABLE} "+
            "(ikey int Primary Key not null unique AUTO_INCREMENT, "+
            "sname varchar(22) not null, "+
            "dbalance decimal(12,2) not null)  ";
  
    var querier = new QueryRunner(ogDb, [sSql]);
    return querier.executeQueries();
  })
  .catchError((oError) =>
    throw("Create '${S_MAIN_TABLE}' - ${oError}"))
  .then((_) {
/*
 * Create 'sequences' table    
 */
    sSql = "CREATE TABLE IF NOT EXISTS sequences "+
            "(iKey int Primary Key not null unique, "+
            "ilastkey int not null, stablename varchar(20) not null)";
    
    var querier = new QueryRunner(ogDb, [sSql]);
    querier.executeQueries();
  })
  .catchError((oError) =>
    throw("Create 'sequences' - ${oError}"))
  .then((_) {
/*
 * Create 'random' table
 */
    sSql = "CREATE TABLE IF NOT EXISTS random "+
            "(iKey int Primary Key not null unique)";
    var querier = new QueryRunner(ogDb, [sSql]);
    querier.executeQueries();
  })
  .catchError((oError) =>
    throw("Create 'random' - ${oError}")) 
  .then((_) {
/*
 *  Create 'totals' table
 */
    sSql = "CREATE TABLE IF NOT EXISTS totals "+
            "(iKey int Primary Key not null unique, "+
            "iinstancenr int not null, itottype TINYINT NOT NULL, "
            "stotdesc varchar(20) not null, "+
            "iinsertnr int not null, iupdatenr int not null, "+
            "dccyvalue decimal(12,2), itotmillis int not null)";
      
    var querier = new QueryRunner(ogDb, [sSql]);
    querier.executeQueries();
  })
  .catchError((oError) =>
    throw("Create 'random' - ${oError}")) 
  .then((_) { 
/*
 * Clean Main table if selected    
 */
    return !tClearMainTable ? true :
      fExecuteSql("TRUNCATE TABLE ${S_MAIN_TABLE}",
                   S_MAIN_TABLE, "fCreateTablesEtc", -1, true);
  })
  .catchError((oError) =>
     throw("Clear '${S_MAIN_TABLE}' ${oError}"))
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fCreateTablesEtc", "failed to Clear "+
                    "'${S_MAIN_TABLE}' table");
/*
 * Clear the 'random' table used for keys to use in update
 */
    return fExecuteSql("TRUNCATE TABLE random", "random",
                        "fCreateTablesEtc", -1, true);
   })
   .catchError((oError) =>
     throw("Truncate 'random' table ${oError}"))
   .then((bool tResult) {
     if (tResult != true)
       fFatal(null, "fCreateTablesEtc",
              "failed to Clear 'random' table"); 
/*
 * Clear the 'totals' table
 */
     return fExecuteSql("TRUNCATE TABLE totals", "totals",
                      "fCreateTablesEtc", -1, true);
   })
   .catchError((oError) =>
     throw("Truncate 'totals' - ${oError}"))
   .then((bool tResult) {
     if (tResult != true)
       fFatal(null, "fCreateTablesEtc",
                     "failed to Truncate 'totals' table");
/*
 *  Delete the rows used for program-generated keys (then re-inserted)
 */
    sSql = "DELETE FROM sequences WHERE ikey = "+
            "$S_SEQUENCE_KEY_MAIN OR iKey = $S_SEQUENCE_KEY_TOTALS";

    return ogDb.query(sSql);    
  })
  .catchError((oError) =>
    throw("Delete 'sequences' - ${oError}")) 
  .then((_) {
/*
 *  Insert into 'sequences' table the row 'totals' program-generated keys
 */
    sSql = "INSERT INTO sequences (ikey, iLastKey, stablename) "+
            "VALUES ($S_SEQUENCE_KEY_TOTALS, 1000, 'totals')";
    return fExecuteSql(sSql, "sequences", "fCreateTablesEtc", 1, true);
  }) 
  .catchError((oError) =>
    throw("Insert 'sequences' for 'totals' ${oError}"))
  .then((_) {
/*
 *  Select from the Main table the last key used (for program-generated keys)
 */    
    sSql = "SELECT MAX(iKey) FROM ${S_MAIN_TABLE}";
    return fProcessSqlSelect (sSql, false, ollClassList);
  })
  .catchError((oError) =>
    throw("Insert 'sequences' - ${oError}")) 
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fCreateTablesEtc",
             "Select of max key from ${S_MAIN_TABLE}) failed. Result = ${tResult}");
    List<List<int>> lliResult = ollClassList.llValue;
    int iLastKey = 0;  // init
    if (lliResult != null && lliResult.length > 0
        && lliResult[0][0] != null) {
      iLastKey = lliResult[0][0];   
    }
    
    if (iLastKey < I_MIN_KEY_MAIN) {
      iLastKey = I_MIN_KEY_MAIN;
    }
/*
 *  Insert into 'sequences' table the row for main table program-generated keys
 */        
    sSql = "INSERT INTO sequences (ikey, iLastKey, stablename) "+
            "VALUES ($S_SEQUENCE_KEY_MAIN, ${iLastKey}, '${S_MAIN_TABLE}')";

    return fExecuteSql(sSql, "sequences", "fCreateTablesEtc", 1, true);
  })
  .catchError((oError) =>
    throw("Insert 'sequences' ${oError}"))
  .then((tResult) {
    if (tResult != true)
      fFatal(null, "fCreateTablesEtc", "Unable to Insert into "+
             "'Sequences' for ${S_MAIN_TABLE}");
    return fInsertOpeningTotals(0);
  })
  .then((_) { oCompleter.complete(true); })
  .catchError((oError) =>
    fFatal(null, "fCreateTablesEtc", "${oError}"));
 
  return oCompleter.future;
}

/*
 **** Insert opening values into 'totals' table ****
 */
async.Future<bool> fInsertOpeningTotals(int iInstanceNr) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  ClassWrapList ollClassList  = new ClassWrapList();
  
/////  fCheckTransactionStatus("fInsertOpeningTotals", true);
  
  ogPrintLine.fPrintForce("Initializing totals table");

  ClassTotals oClassTotals = new ClassTotals();
  String sSql = "SELECT count(*), sum(dbalance) FROM ${S_MAIN_TABLE}";
  
  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "fInsertOpeningTotals",
             "Select of ${S_MAIN_TABLE} failed. Result = ${tResult}");
    
    List<List> llResult = ollClassList.llValue;
    
    double dCcyTotal = 0.00;   // init
    int iInsertNr    = 0;      // init
    int iUpdateNr    = 0;      // init
    int iCcyTotal    = 0;      // init
    
    if (llResult != null && llResult.length > 0) {
      iInsertNr     = llResult[0][0];  // number in table
      dCcyTotal     = llResult[0][1];  // values in table
      if (iInsertNr == null)
        iInsertNr = 0;
      if (dCcyTotal != null)
        iCcyTotal = ogCcy.fCcyDoubleToInt(dCcyTotal);
    }
    
    oClassTotals.fSetValues (iInstance    : iInstanceNr,
                             iTotType     : I_TOT_OPEN,
                             iInsertTotNr : iInsertNr,
                             iUpdateTotNr : 0,
                             iCcyTotAmt   : iCcyTotal,
                             iTotMillis   : 0);
 
    oClassTotals.fPrint();
    
    fInsertIntoTotalsTable (oClassTotals)
    .then((bool tResult) {
      if (tResult != true)
        fFatal(null, "fCreateTablesEtc", "Insert into totals (Opening) failed");
      oCompleter.complete(tResult);
      return;
    });
  })
  .catchError((oError) {
    fFatal(null, "fInsertOpeningTotals", "${oError}");
  });
       
  return oCompleter.future; 
}

/*
 **** Insert into 'totals' table values from inserts and updates ****
 */
async.Future<bool> fInsertIntoTotalsTable(ClassTotals oClassTotals) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

////  fCheckTransactionStatus("fInsertIntoTotalsTable", true);
  
  String sSql   = oClassTotals.fFormatSql();
  
  fInsertRowWithSequence("totals", S_SEQUENCE_KEY_TOTALS, sSql, false, null)
  .then((tResult) {
    if (tResult != true)
      throw("Insert failed");
    else
      oCompleter.complete(true);
  })
  .catchError((oError) =>
    fFatal(null, "fInsertIntoTotalsTable", "${oError}"));
  
  return oCompleter.future;  
}

/*
 **** Execute a single SQL string supplied. ****
 */
async.Future<bool> fExecuteSql(String sSql, String sTableName,
    String sCalledBy, int iRequiredRows, bool tShowError) {
    
  async.Completer<bool> oCompleter = new async.Completer<bool>();

////  fCheckTransactionStatus("fExecuteSql:${sCalledBy}:", true);
  var oTxn;
    
  ogDb.startTransaction()
  .then((oResult) {
    oTxn = oResult;
    return oTxn.query(sSql)
    .then((oResult) {
      if (iRequiredRows > -1 && oResult.affectedRows != iRequiredRows)
        throw("Rows affected = ${oResult.affectedRows}");
      return oTxn.commit();
    })
    .then((_) =>
      oCompleter.complete(true));
  })
  .catchError((oError) {
    if (tShowError)
      print ("Error in fExecuteSql: table ${sTableName}, "+
              "called by ${sCalledBy}, Error = ${oError}");
    fRollback(oTxn).then((tResult) {
      if (tShowError)
        print ("Update failed and Rollback completed");
      oCompleter.complete(false);
    });
  });  
  return oCompleter.future;
}
  
/*
 **** Attempt to connect to specific port to determine if first process. ****
 */
async.Future<bool> fCheckIfFirstInstance() {
  async.Completer<bool> oCompleter = new async.Completer<bool>(); 
  RawServerSocket.bind("127.0.0.1", 8087)
  .then((oSocket) {
    ogSocket = oSocket;         // assign to global
    oCompleter.complete(true);
  })
  .catchError((oError) {
    oCompleter.complete(false);
  });
  
  return oCompleter.future;
}

/*
 **** Class To Store and Print Totals
 */
class ClassTotals{
  int    _iInstance;
  int    _iTotType;
  int    _iInsertNr;
  int    _iUpdateNr;
  int    _iCcyTotAmt;
  int    _iTotMillis;
  List<String> _lsTotDesc = ["Opening Values", "Inserted Values", "Updated Values"];  
  
  ClassTotals() {}
  
  void fSetValues ({int iInstance, int iTotType, int iInsertTotNr, int iUpdateTotNr,
                    int iCcyTotAmt, int iTotMillis}) {
    _iInstance  = iInstance;
    _iTotType   = iTotType;
    _iInsertNr  = iInsertTotNr;
    _iUpdateNr  = iUpdateTotNr;
    _iCcyTotAmt = iCcyTotAmt;
    _iTotMillis = iTotMillis;
  }
  
  void fPrint() {
    String sTotDesc = _lsTotDesc[_iTotType];
    int iTotNr = _iInsertNr > 0 ? _iInsertNr : _iUpdateNr;
    String sAverageMillis = "";
    if (_iTotMillis > 0 && iTotNr > 0)
      sAverageMillis = ", Avg Millis = "
                        +(_iTotMillis / iTotNr).toStringAsFixed(2);
    String sFmtCcy = ogFormatCcy.fFormatCcy(ogCcy.fCcyIntToDouble(_iCcyTotAmt));
  
    String sPline = "${sTotDesc}, Nr: ${iTotNr}, Val: ${sFmtCcy}"+sAverageMillis;
    ogPrintLine.fPrintForce(sPline);
  }
  
  String fFormatSql() {
    String sCcyValue = ogCcy.fCcyIntToString(_iCcyTotAmt);
    String sTotDesc  = _lsTotDesc[_iTotType];
    return    "(iKey, iinstancenr, itottype, stotdesc, "+
               "iinsertnr, iUpdateNr, dccyvalue, itotmillis) VALUES ( "+
               "?, ${_iInstance}, ${_iTotType}, '${sTotDesc}', ${_iInsertNr}, "+
               "${_iUpdateNr}, ${sCcyValue}, ${_iTotMillis})";
  }  
}

/*
 **** Class to handle currency as double, integer, and string 
 */
class ClassCcy {
  int    _iDecPlaces;
  int    _iScale     = 1;  // init

  ClassCcy (int iDecPlaces) {    
    _iDecPlaces = iDecPlaces;
    for (int i=0; i<iDecPlaces; i++, _iScale *= 10);
  }  
  
  double fAddCcyDoubles (double dAmt1, double dAmt2) {
    double dResult = ((fCcyDoubleToInt(dAmt1) + fCcyDoubleToInt(dAmt2)) / _iScale);
    return double.parse(dResult.toStringAsFixed(_iDecPlaces));
  }

  int fCcyStringToInt (String sAmt)
    => fCcyDoubleToInt(double.parse(sAmt));
  
  int fCcyDoubleToInt(double dAmt) =>
    int.parse((dAmt.toStringAsFixed(_iDecPlaces)).replaceFirst(".", ""));
  
  double fCcyIntToDouble(int iAmt) =>
    double.parse((iAmt/_iScale).toStringAsFixed(_iDecPlaces));
  
  String fCcyIntToString(int iAmt)
    => ((iAmt/_iScale).toStringAsFixed(_iDecPlaces));
}

/*
 **** Class to format Currency Value with punctuation (probably not needed with intl)
*/
class ClassFormatCcy {
  int    _iDecPlaces;
  String _sCcySymbol;
  String _sDecSep;
  String _sThouSep;
  
  ClassFormatCcy (int iDecPlace, String sDecSep, String sThouSep, sCcySymbol) {
    _iDecPlaces = iDecPlace;
    _sCcySymbol = sCcySymbol;
    _sDecSep    = sDecSep;
    _sThouSep   = sThouSep;
  }
  
  String fFormatCcy(double dMoney) {
    String sMoney = dMoney.toStringAsFixed(_iDecPlaces);
    StringBuffer sbMoney = new StringBuffer();
    int iDecPos = sMoney.indexOf(_sDecSep);
    if (iDecPos < 0)   // no decimal place
      iDecPos = sMoney.length;
    for (int iPos = 0; iPos < sMoney.length; iPos++) {
      if ((iPos != 0) && (iPos < iDecPos) && (iDecPos -(iPos)) % 3 == 0)
        if (!(iPos == 1 && sMoney[0] == "-"))
          sbMoney.write(_sThouSep);
      sbMoney.write(sMoney[iPos]);
    }
    return _sCcySymbol +sbMoney.toString();
  }
}

class ClassWrapInt {
  int _iValue;
  
  ClassWrapInt() {}
  
  int  get iValue => _iValue;
  
  void set iValue(int iNewValue) {_iValue = iNewValue;}
}

class ClassWrapList {
  List<List> _llValue;
  
  ClassWrapList() {}
  
  List<List> get llValue => _llValue;
  
  void       set llValue(List<List> llNewValue) {_llValue = llNewValue;}
}

class ClassWrapString {
  String _sValue;
  
  ClassWrapString() {}
  
  String get sValue => _sValue;
  
  void   set sValue(String sNewValue) {_sValue = sNewValue;}
}

/*
 **** Class To Print a single line To Console (can probably go)
 */
class ClassPrintLine {
  bool   _tPrint;
  int    _iLineNr  = 0;
  String _sNewline = "\n";
  
  ClassPrintLine(bool tPrint) {_tPrint = tPrint;}
  
  void set tPrint (bool tPrint) {_tPrint = tPrint;}
  
  void fWriteForce(String sPline) {
    _sNewline = "";
    fPrintForce(sPline);
    _sNewline = "\n";    
  }
  
  void fPrintForce (String sPline) {
    bool tPrintSave = _tPrint;  // save existing variable
    tPrint = true;
    fPrint (sPline);
    _tPrint = tPrintSave;
  }
  
  void fPrint (String sPline) {
    if (_tPrint) {
      String sMsg = (++_iLineNr).toString();
      if (_iLineNr < 10)  // line sequence number
        sMsg = "0"+sMsg;
      stdout.write(sMsg+"   "+sPline+_sNewline);
    }
  }
}

/*
 **** Class to Create Random Names For Inserts
 */
class ClassRandNames {
  int     _iNamesUsed;
  List<String>    _lsNamesAvail = ['Kasper', 'Lars', 'Anders', 'James',
                  'Brendan', 'Ken', 'Dermot', 'Monty', 'James', 'Dennis',
                  'Seth', 'Alice', 'Drew', 'Alex', 'Ross', 'George',
                  'Carole', 'Rhonda', 'Kerry', 'Linus', 'Marilyn',
                  'Marcus', 'Rita', 'Barbara', 'Kevin', 'Brian'];
  List<bool> _ltNamesUsed;
  Random  _oRandom  = new Random();

  ClassRandNames() {
    _ltNamesUsed = new List<bool>(_lsNamesAvail.length); // list of used names
    _iNamesUsed = 0;
  }
  
/*
 * Get a Single Random Name
 */
  String fGetRandName() {   // Get a new name from list
    int iPos;
    for (bool tUsed = true; tUsed;) {
      iPos = _oRandom.nextInt(_lsNamesAvail.length);
      tUsed = (_ltNamesUsed[iPos] == true);
      if (tUsed && _iNamesUsed >= _lsNamesAvail.length-7) {  // leave space for speed
        _ltNamesUsed = new List<bool>(_lsNamesAvail.length); // list of used names
        _iNamesUsed = 0;
        tUsed = false;
      }
    }
    _iNamesUsed++;
    _ltNamesUsed[iPos] = true;
    return _lsNamesAvail[iPos];
  }
}

/*
 ****   Exit this program ****
 */
void fExit(int iCode) {
  if (ogDb != null)
    ogDb.close();

  exit(iCode);
}

/*
 **** ClassTerminalInput : Class specific to this application for terminal input
 */
class ClassTerminalInput {
  List<String>   _lsHeading           = new List(6);
  List<String>   _lsPrompts           = new List(I_MAX_PROMPT+1);   // Prompt strings
  List<String>   _lsFieldNames        = new List(I_MAX_PROMPT);
  List<List>     _lliMinMax           = new List(I_MAX_PROMPT+1);
  List<List>     _llsAllowableEntries = new List(I_MAX_PROMPT+1);   // Allowable entries
  List<Function> _lfValidation        = new List(I_MAX_PROMPT+1);   // validation functions
  ClassConsole   _oClassConsole       = new ClassConsole();         // general class
   
  ClassTerminalInput() {
    _lsPrompts[I_DB_USER]       = "Database User Name .............. : ";
    _lsPrompts[I_DB_PWD]        = "Database User Password .......... : ";
    _lsPrompts[I_DB_NAME]       = "Database Name ................... : ";
    _lsPrompts[I_MAX_INSERTS]   = "Number of Insert Iterations ..... : ";
    _lsPrompts[I_INS_ROLLBACKS] = "Frequency of Insert Rollbacks ... : ";    
    _lsPrompts[I_USE_AUTOINC]   = "Use AutoInc for Inserts (y/n) ... : ";
    _lsPrompts[I_MAX_UPDATES]   = "Number of Update Iterations ..... : ";
    _lsPrompts[I_UPD_ROLLBACKS] = "Frequency of Update Rollbacks ... : ";    
    _lsPrompts[I_SELECT_SIZE]   = "Select sample-size for updates .. : ";
    _lsPrompts[I_CLEAR_YN]      = "Clear Table? (y/n) .............. : ";
    _lsPrompts[I_SAVE_YN]       = "Save selections to file? (y/n) .. : ";
    _lsPrompts[I_INSTANCE_TOT]  = "Nr. of instances you will run ... : ";
    _lsPrompts[I_CORRECT_YN]    = "Details Correct? (y/n/end) ...... : ";
    
    _lsFieldNames[I_DB_USER]       = "Database User Name";
    _lsFieldNames[I_DB_PWD]        = "Database User Password";
    _lsFieldNames[I_DB_NAME]       = "Database Name";
    _lsFieldNames[I_MAX_INSERTS]   = "Number of Insert Iterations";
    _lsFieldNames[I_INS_ROLLBACKS] = "Frequency of Insert Rollbacks";    
    _lsFieldNames[I_USE_AUTOINC]   = "Use AutoInc for Inserts (y/n)";
    _lsFieldNames[I_MAX_UPDATES]   = "Number of Update Iterations";
    _lsFieldNames[I_UPD_ROLLBACKS] = "Frequency of Update Rollbacks";    
    _lsFieldNames[I_SELECT_SIZE]   = "Select sample-size for Updates";
    _lsFieldNames[I_CLEAR_YN]      = "Clear Table? (y/n)";
    _lsFieldNames[I_SAVE_YN]       = "Save selections to file? (y/n)";
    _lsFieldNames[I_INSTANCE_TOT]  = "Number of instances you will run";
 
    _lliMinMax[I_DB_USER]       = [1];
    _lliMinMax[I_DB_PWD]        = [1];
    _lliMinMax[I_DB_NAME]       = [1];
    _lliMinMax[I_MAX_INSERTS]   = [0, 1000000];
    _lliMinMax[I_INS_ROLLBACKS] = [0, 10000];    
    _lliMinMax[I_USE_AUTOINC]   = [1];
    _lliMinMax[I_MAX_UPDATES]   = [0, 1000000];
    _lliMinMax[I_UPD_ROLLBACKS] = [0, 10000];   
    _lliMinMax[I_SELECT_SIZE]   = [1, 1000];
    _lliMinMax[I_CLEAR_YN]      = [1];
    _lliMinMax[I_SAVE_YN]       = [1];
    _lliMinMax[I_INSTANCE_TOT]  = [1,999];
    _lliMinMax[I_CORRECT_YN]    = [1];
    
    _llsAllowableEntries[I_USE_AUTOINC] = ["y", "n"];
    _llsAllowableEntries[I_CLEAR_YN]    = ["y", "n"];
    _llsAllowableEntries[I_SAVE_YN]     = ["y", "n"];
    _llsAllowableEntries[I_CORRECT_YN]  = ["y", "n"];
    
    List<String> _lsDataTypes = new List(I_MAX_PROMPT+1);
    _lsDataTypes[I_MAX_INSERTS]   = "int";
    _lsDataTypes[I_INS_ROLLBACKS] = "int";    
    _lsDataTypes[I_MAX_UPDATES]   = "int";
    _lsDataTypes[I_UPD_ROLLBACKS] = "int";    
    _lsDataTypes[I_SELECT_SIZE]   = "int";
    _lsDataTypes[I_INSTANCE_TOT]  = "int";
       
    _lsHeading[0] = "";
    _lsHeading[1] = "";
    _lsHeading[2] = "Program to test Dart 'MySql' package (sqljocky) by "+
                     "JamesOts (on Github)";
    _lsHeading[3] = "";
    _lsHeading[4] = "Enter required parameters as follows";
    _lsHeading[5] = "------------------------------------";
    
    _oClassConsole.fInit(I_MAX_PROMPT, _lsPrompts, _lsDataTypes,
                         _llsAllowableEntries, _lfValidation, _lsHeading,
                         _lliMinMax);
  }

/*
 * ClassTerminalInput: Prompt User for parameters
 */
  List<String> fGetUserSelections() {
    int  iDisplayLine;
    String sOldStartTime;
       
    // Read parameters from file
    List<String> lsInput = _oClassConsole.fReadParamsFile(
                           S_PARAMS_FILE_NAME, I_TOTAL_PROMPTS);
    
    // save parameters for later comparison    
    List<String> lsSavedParams = new List(lsInput.length);
    for (int i=0; i < lsInput.length-1; lsSavedParams[i]=lsInput[i], i++);

    // determine validity of data on file and get first invalid line
    int iPromptNr = _oClassConsole.fValidateAllInput(lsInput, I_TOTAL_PROMPTS);  
    if (iPromptNr < I_MAX_PROMPT)  // if file data is invalid start at zero
      iPromptNr = 0;
    
    _oClassConsole.fDisplayInput(iPromptNr-1, lsInput);  // display previous lines

    bool tValid = false;    
    while (!(tValid)) {
      _oClassConsole.fGetUserInput(iPromptNr, lsInput, "end");
      
      print ("\n\n\n");     
      tValid = (lsInput[I_CORRECT_YN] == "y");
      lsInput[I_CORRECT_YN] = "";   // clear entry so not defaulted
      iPromptNr = 0;   // init
      if (tValid) {
        // check for first invalid line
        iPromptNr = _oClassConsole.fValidateAllInput(lsInput, I_TOTAL_PROMPTS);
        tValid = (iPromptNr >= I_CORRECT_YN);

        if (! tValid)
          print ("Entry is required for : "+ _lsFieldNames[iPromptNr]);
      } else {   
        // re-read file and if no changes made here, replace parameters
        bool tReplaced = _fCheckFileValues(lsInput, lsSavedParams);
        if (tReplaced)
          iPromptNr = I_CORRECT_YN;  // redisplay values
      }        
      if (tValid) {
        for (iPromptNr = 0; iPromptNr < I_INSTANCE_TOT && tValid;
        tValid = (lsInput[iPromptNr] != ""), iPromptNr++);
        
        if (! tValid)
          print ("\nParameter ${_lsFieldNames[iPromptNr]} "+
                 "must be entered");
      }
      
      if (tValid) {
        int iTemp = int.parse(lsInput[I_INSTANCE_TOT], onError: (_) {
          tValid = false;
        });
     ////   try {
     ////     int iTemp = int.parse(lsInput[I_INSTANCE_TOT]);
     ////     tValid = (iTemp > 0);
     ////   } catch (oError) {tValid = false;}

        if (!tValid) {
          print ("Invalid - at least one instance must run");
        } else if ((int.parse(lsInput[I_MAX_INSERTS]) < 1) &&
         lsInput[I_CLEAR_YN] == "y" &&
         int.parse(lsInput[I_MAX_UPDATES]) > 0) {
          print ("Invalid - there will be nothing to update (zero inserts)");
          tValid = false;
          iPromptNr = I_CLEAR_YN;       
        }
      }
    }
      
    if (lsInput[I_SAVE_YN] == 'y')  // then save data
      _oClassConsole.fSaveParams(_lsPrompts, lsInput,
        I_MAX_PROMPT, S_PARAMS_FILE_NAME);
    
    return lsInput;
  }
  
/*
 * ClassTerminalInput: Check if parameters entered are valid
 */
  int _fValidateParams(List<String> lsInput, bool tChangeTime) {    
    int iMax = I_INSTANCE_TOT;
    for (int iPos = 0; iPos <= I_INSTANCE_TOT; iPos++) {
      if (lsInput[iPos] == "")
        return iPos;
    }
    return I_MAX_PROMPT;
  }
  
/*
 * ClassTerminalInput: if no alterations, re-read file
 */
  bool _fCheckFileValues (List<String> lsInput, List<String> lsSavedParams) {

    bool tAltered = false;

    for (int i=0; i<=(I_MAX_PARAM) && !tAltered;
     tAltered = (lsInput[i] != lsSavedParams[i]), i++);
 
    if (tAltered)     // there has been new input by operator
      return false;   // not replaced
     
    List<String> lsNewParams = _oClassConsole.fReadParamsFile(
                               S_PARAMS_FILE_NAME, I_TOTAL_PROMPTS);
 
    tAltered = false;   // init
    for (int i=0; i<=(I_MAX_PARAM) && !tAltered;
     tAltered = (lsInput[i] != lsNewParams[i]), i++);
    
    if (!tAltered)     // values on file are the same
      return false;

    print("");        
    String sInput;
    while (sInput != "y") {
      stdout.write ("\nParameter values on file have altered. "+
             "Do you wish to use them? (y/n)");
      sInput = stdin.readLineSync();
      if (sInput == "end")
        fExit(0);
      if (sInput == "n")
        return false ;    // not replaced;
    }

    // Replace the input parameters with the file parameters //
    for (int i=0; i <= I_MAX_PARAM; lsInput[i] = lsNewParams[i], i++);

    return true;   // parameters were replaced
  }  
}

/*
 **** ClassConsole - Class to Handle Console Input
 */
class ClassConsole {
  int            _iMaxPrompt;
  List<Function> _lfValidation;
  List<List>     _llsValidEntries;
  List<List>     _lliMinMax;
  List<String>   _lsPrompts;
  List<String>   _lsDataTypes;
  List<String>   _lsHeading;

  /*
   * ClassConsole - Initialize
   */
  ClassConsole() {}
  
  fInit(int iMaxPrompt, List<String> lsPrompts, List<String> lsDataTypes,
        List<List> llsValidEntries, List<Function> lfValidation,
        List<String> lsHeading, List<List> lliMinMax) {
    _iMaxPrompt      = iMaxPrompt;
    _lsPrompts       = lsPrompts;
    _lsDataTypes     = lsDataTypes;
    _llsValidEntries = llsValidEntries;
    _lfValidation    = lfValidation;
    _lsHeading       = lsHeading;
    _lliMinMax       = lliMinMax;
  }
  
/*
 * ClassConsole - prompt for user input (all lines)
 */
  void fGetUserInput(int iPromptNr, List<String> lsInput, String sTerminate) {
    Function fLoop;      
     /*
     *  Loop prompting the user for each line
     */
    fLoop = (() {
      // Prompt for one line //
      iPromptNr = _fPromptOneLine(iPromptNr, lsInput, sTerminate); 

      if (iPromptNr > _iMaxPrompt)
        return;

      if (iPromptNr < 0) // 1st valid prompt is "0"
        iPromptNr = 0;

      return fLoop();
    });
    return fLoop();
  }  
  
/*
 * ClassConsole: Prompt User For Parameters (one line)
 */
  int _fPromptOneLine (int iPromptNr, List<String> lsInput, String sTerminate) {

    if (iPromptNr == _iMaxPrompt)
      fDisplayInput(_iMaxPrompt-1, lsInput);
    else if (iPromptNr == 0) {   // 1st line so display heading
      print("");
      for (int i = 0; i < _lsHeading.length; print(_lsHeading[i]), i++);
      for
        (int i=0; i<59; stdout.write(" "), i++);
      print ("Default");
      for
        (int i=0; i<59; stdout.write(" "), i++);
      print ("-------");
    }

    String sDefault = lsInput[iPromptNr] == null ? "" : lsInput[iPromptNr];
    
    String sInput = _fReadConsoleLine(iPromptNr, sDefault, "end");

    if (sTerminate != "" && (sInput == sTerminate ||
        sInput.toLowerCase() == sTerminate)) {
      fExit(0);
    } else if (sInput == "<") {
      iPromptNr -= 2;
    } else {
      lsInput[iPromptNr] = sInput;   // save data entered
    }
    return ++iPromptNr;                  // increment prompt number
   }
  
/*
 * ClassConsole - Read Single Console Line
 */
  String _fReadConsoleLine(int iPromptNr, String sDefault,
                            String sTerminate) {
    
    List<int> liCharCodes       = [];
    String sPrompt              = _lsPrompts[iPromptNr];
    
    while (true) {
      liCharCodes.clear();
      stdout.write("\n$sPrompt");
      if (sDefault != "") {
        // Display the default //
        for
          (int i=sPrompt.length; i<60; stdout.write(" "), i++);
        stdout.write("$sDefault");
        // backspace to position //
        for
          (int i = 58+sDefault.length+2; i>sPrompt.length; stdout.write("\b"), i--);
      }
      int iInput;
      while (iInput != 10) {   // 10 = newline
        // I'll alter this to read raw bytes when available //
        iInput = stdin.readByteSync();
        if (iInput > 31)  // then ascii printable char
          liCharCodes.add(iInput);
      }
      int iSecs = 0;    // init
      String sInput = new String.fromCharCodes(liCharCodes);
 
      if (sInput == "")
        sInput = sDefault;
      else
        sInput = sInput.trim();
      
      String sErrorLine = "";      
      try {
        if (sTerminate != "" && (sInput == sTerminate ||
            sInput.toLowerCase() == sTerminate)) {
        } else if (sInput == "<") {  // back one
        } else {
          sErrorLine = _fValidateSingleLine(iPromptNr, sInput);
        }
      } catch (oError) {
        sErrorLine = "$oError";
      }
      
      if (sErrorLine == "")
        return sInput;
      
      print (sErrorLine +"\n");       
    }
  }
   
/*
 * ClassConsole - Validate Single input
 */
  String _fValidateSingleLine (int iPromptNr, String sInput) {
    int iMinVal;
    int iMaxVal;
    String sDataType            = _lsDataTypes[iPromptNr];  // default is String
    String sErrorLine;
    
    if (_lliMinMax[iPromptNr] != null && 
        _lliMinMax[iPromptNr].length > 0) {
      iMinVal = _lliMinMax[iPromptNr][0];
      if (_lliMinMax[iPromptNr].length > 1)
        iMaxVal = _lliMinMax[iPromptNr][1];
    }
    
    List<String> lsValidEntries = _llsValidEntries[iPromptNr];
    Function fValidation        = _lfValidation[iPromptNr];  
  
    if (fValidation != null) {
      sErrorLine = fValidation(sInput);
    } else if (sDataType == "int") {
      sErrorLine = _fValidateInt(sInput, iMinVal, iMaxVal, lsValidEntries);
    } else
      sErrorLine = _fValidateString(sInput, iMinVal, iMaxVal, lsValidEntries);          
 
    return sErrorLine;
  }
  
/*
 * ClassConsole - Validate String Entered
 */
  String _fValidateString (String sInput, int iMinLgth, int iMaxLgth, List lsAllowableEntries) {

    if (iMinLgth != null && sInput.length < iMinLgth) {
      if (iMinLgth == 1)
        return ("Invalid - field is compulsory");
      return ("Invalid - minimum length for field is ${iMinLgth}");
    }
    
    if (iMaxLgth != null && iMaxLgth > 0 && sInput.length > iMaxLgth)
      return ("Invalid - maximum length for field is ${iMaxLgth}");
      
    bool tValid = (lsAllowableEntries == null || lsAllowableEntries.length < 1);
    if (!tValid && lsAllowableEntries.length == 2 &&
        lsAllowableEntries[0] == "y" && lsAllowableEntries[1] == "n")
      sInput = sInput.toLowerCase();
    for (int iPos = 0; !tValid && iPos < lsAllowableEntries.length; iPos++)
      tValid = (sInput == lsAllowableEntries[iPos]);

    return tValid ? "" : "Invalid - entry must be one of ${lsAllowableEntries}";
  }
 
/*
 * ClassConsole - Validate Integer Entered
 */
  String _fValidateInt(String sInput, int iMinVal, int iMaxVal, List lsAllowableEntries) {
    int iInput;
////    iInput = int.parse(sInput, onError: (_) {
////     return "Invalid - non-numeric entry - '$sInput'";
////    });
    try {
      iInput = int.parse(sInput);
    } catch (oError) {
      return "Invalid - non-numeric entry - '$sInput'";
    }
    if (iMinVal != null && iInput < iMinVal)
      return "Invalid - Entry must not be less than ${iMinVal}";
    if (iMaxVal != null && iInput > iMaxVal)
      return "Invalid - Entry must not exceed ${iMaxVal}";

    bool tValid = (lsAllowableEntries == null || lsAllowableEntries.length < 1);
    for (int iPos = 0; !tValid && iPos < lsAllowableEntries.length; iPos++)
      tValid = (iInput == lsAllowableEntries[iPos]);

    return (!(tValid)) ? "Invalid - entry must be one of ${lsAllowableEntries}" : "";
  }
    
/*
 ** ClassConsole: Save Selections to File
 */
  void fSaveParams(List<String> lsPrompts, List<String> lsInput,
    iMaxParams, sFileName) {
    String sFileBuffer = "";        // used to write to file
    for (int iPos1 = 0; iPos1 < iMaxParams; iPos1++) {
      sFileBuffer = sFileBuffer +"${_lsPrompts[iPos1]}";
      sFileBuffer = sFileBuffer +"${lsInput[iPos1]}\n";
    }
    ogPrintLine.fPrintForce ("Saving selections");
    new File(sFileName).writeAsStringSync(sFileBuffer);
    ogPrintLine.fPrintForce ("File written");
  }
  
/*
 * ClassConsole: Read Parameters from File
 */
  List<String> fReadParamsFile(String sFileName, int iTotLines) {
    
    List<String> lsInput = new List(iTotLines);
    
    fReadFixedListSync(sFileName, lsInput);
    // extract the parameters for the file
    for (int iPos1 = 1; iPos1 <= iTotLines; iPos1++) {
      String sParam = lsInput[iPos1-1];
      int iPos2 = sParam.indexOf(" :");
      if (iPos2 < 1)  // no default
        lsInput[iPos1-1] = "";
      else
        lsInput[iPos1-1] = sParam.substring(iPos2+2).trim();
    }   
    
    return lsInput;
  }
  
/*
 * ClassConsole: Read fixed-length String List from file
 */
  int fReadFixedListSync(String sFileName, List<String> lsData) {
    int iLinesOnFile = 0;
    int iByte;
    List<int> lCharCodes = [];
    try {
      var fileSync = new File(sFileName).openSync(mode: FileMode.READ); 
      do {  
        iByte = fileSync.readByteSync();
        if (iByte > 31)   // printable
          lCharCodes.add(iByte);
        else if (iByte == 10) {   // part of CR/LF
          iLinesOnFile++;
          if (iLinesOnFile <= lsData.length)
            lsData[iLinesOnFile-1] = new String.fromCharCodes(lCharCodes);
          lCharCodes.clear();
        }
      } while (iByte > -1);
    } catch(oError){
      if (!(oError is FileException))
        throw ("fReadFixedListSync: Error = \n${oError}");
    }
// Make remaining lines (if any) Strings //
    for (int i=iLinesOnFile+1; i<=lsData.length; lsData[i-1] = "", i++);
    return iLinesOnFile;   // return actual line count
  }
  
/*
 * Class Console : Validate all data in List
 */
  int fValidateAllInput(List lsData, int iMaxPrompt) {
    int iPromptNr;
    for (iPromptNr = 0; iPromptNr <= iMaxPrompt; iPromptNr++) {
      String sErmes = _fValidateSingleLine (iPromptNr, lsData[iPromptNr]);
      if (sErmes != "")   // invalid
        return iPromptNr;
    }
    return iMaxPrompt +1;   // all valid
  } 
  
/*
 * Class Console: Display Selections made by Operator
 */
  void fDisplayInput(iMaxPrompt, List<String> lsInput) {
    for (int i = 0; i < _lsHeading.length; print(_lsHeading[i]), i++);
    for (int iPos = 0; iPos <= iMaxPrompt; iPos++){
      print ("");
      print (_lsPrompts[iPos] +"${lsInput[iPos]}");
    }   
  }
}