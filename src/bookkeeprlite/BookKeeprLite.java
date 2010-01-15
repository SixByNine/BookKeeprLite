/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 *
 * @author kei041
 */
public class BookKeeprLite {

    final String dbname;

    /**
     * Creates a new instance of BookKeeprLite for operations.
     *
     * Starts the SQLite database driver and opens a connection.
     * 
     * @param dbname the SQLite database to use
     * @param logfile a file used to write logs.
     */
    public BookKeeprLite(final String dbname, String logfile) throws BookKeeprLiteException {
        this.dbname = dbname;

        // Set up logging.
        final Logger logger = Logger.getLogger("bookkeeprlite");
        try {
            Handler h = new FileHandler(logfile);
            h.setFormatter(new LogFormatter());
            logger.addHandler(h);
        } catch (IOException ex) {
        }
        Logger.getLogger(BookKeeprLite.class.getName()).info("Starting bookkeeprlite");

        // load the SQLite driver.
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(BookKeeprLite.class.getName()).log(Level.SEVERE, null, ex);
        }


    }

    /**
     * does a very simple test to see if a table can be created, keys entered
     * and read back from the table.
     *
     * @throws bookkeeprlite.BookKeeprLiteException If there is an error whilst reading or writing to the database.
     */
    public void test() throws BookKeeprLiteException {
        Connection conn = null;

        // Try and connect to the database.
        try {
            conn = this.connectDB();

            conn.setAutoCommit(false);

            Statement s = conn.createStatement();
            synchronized (this) {
                s.executeUpdate("drop table if exists `test`;");
                s.executeUpdate("create table `test` (key,val);");

                conn.commit();
            }
            for (int i = 0; i < 100; i++) {
                s.addBatch("insert into `test` values ('k" + i + "','v" + i + "');");
            }

            synchronized (this) {
                s.executeBatch();
                conn.commit();
            }

            synchronized (this) {
                ResultSet rs = s.executeQuery("select * from `test`;");

                int i = 0;
                while (rs.next()) {
                    if (!rs.getString("key").equals("k" + i)) {
                        throw new BookKeeprLiteException("Error, could not get the correct keys back from the dabase");
                    }
                    i++;
                }
                rs.close();

                s.executeUpdate("drop table if exists `test`;");
                conn.commit();
                
            }

            synchronized (this) {

                s.execute("select count(*) from `pointings`;");
//                ResultSet rs = s.executeQuery("select count(*) from `pointings`;");
                ResultSet rs = s.getResultSet();
                int nptg = -1;
                if (rs.next()) nptg = rs.getInt(1);
                rs.close();
                Logger.getLogger(BookKeeprLite.class.getName()).info("Found "+nptg+" pointings in the database");
            }

            conn.close();

        } catch (SQLException ex) {
            throw new BookKeeprLiteException("An error occured trying to test the dabase", ex);
        }

        Logger.getLogger(BookKeeprLite.class.getName()).info("Test shows database can be read and written ok");
    }

    public void initialiseBookKeepr() throws BookKeeprLiteException {
        Logger.getLogger(BookKeeprLite.class.getName()).info("Initialising the database, this will remove all entries!");

        Connection conn = null;
        // Try and connect to the database.
        try {
            conn = this.connectDB();

            conn.setAutoCommit(false);

            Statement s = conn.createStatement();
            synchronized (this) {
                s.executeUpdate("drop table if exists `pointings`;");
                s.executeUpdate("create table `pointings` (" +
                        "`uid` integer primary key," +
                        "`gridid`, `coordinate`, `rise`, `set`, `toobserve`, `survey` , `region`, `tobs`, `config`,`ra`,`dec`,`gl`,`gb`);");

                conn.commit();

                s.executeUpdate("drop table if exists `psrxml`;");
                s.executeUpdate("create table `psrxml` (" +
                        "`uid` integer primary key, `pointing_uid`" +
                        "`url`, `coordinate`,`ra`,`dec`,`gl`,`gb`,`utcstart`,`lst`,`beam`,`tobs`,`source_id`,`programme`);");

                conn.commit();
            }
            conn.close();
        } catch (SQLException ex) {
            throw new BookKeeprLiteException("An error occured trying to initialise the database", ex);
        }
        Logger.getLogger(BookKeeprLite.class.getName()).info("Database initialisation complete");
    }

    /**
     *
     * Connects to the SQLite database.
     *
     * @return A connection to the database.
     * @throws java.sql.SQLException
     */
    public Connection connectDB() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbname);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        boolean quiet = false;
        boolean interactive = false;
        boolean reinit = false;

        for(String a : args){
            String shortargs="";
            if(a.startsWith("-") && a.charAt(1) != '-')shortargs=a;
            if(a.equals("--quiet") || shortargs.contains("q"))quiet=true;
            if(a.equals("--reinit"))reinit=true;
            if(a.equals("--interactive") || shortargs.contains("i"))interactive=true;
        }

        if (!interactive) {
            try {
                System.in.close();
            } catch (IOException ex) {
            }
        }

        try {
            Logger.getLogger(BookKeeprLite.class.getName()).info("(startup) Attempt to start a bookkeepr instance");
            BookKeeprLite bk = new BookKeeprLite("test.db", "bookkeepr.log");
            Logger.getLogger(BookKeeprLite.class.getName()).info("(startup) bookkeepr instance started ok");


            if (reinit) {
                Logger.getLogger(BookKeeprLite.class.getName()).warning("(startup) re-initialising the database: This removes all contents!!");
                bk.initialiseBookKeepr();
            }

            if (quiet) {
                Logger.getLogger(BookKeeprLite.class.getName()).info("(startup) quiet mode, closing output");
                System.out.close();
                System.err.close();
            }

            bk.test();


            if (interactive) {
                InteractiveConsole.interactiveConsole(bk);
            }


        } catch (BookKeeprLiteException ex) {
            Logger.getLogger(BookKeeprLite.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        }

    }
}
