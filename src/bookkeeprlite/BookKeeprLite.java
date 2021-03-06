/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import bookkeeprlite.sqlitefunctions.GalacticDistanceComparatorDBFunction;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * BookKeeprLite: A light version of the BookKeepr using SQLite as the backend
 * database.
 *
 * Uses tables: pointings, beams, psrxml.
 *
 * @author kei041
 */
public class BookKeeprLite {

    private final Logger logger = LoggerFactory.getLogger(BookKeeprLite.class);
    private final String dbname;
    private final LogFormatter logFormatter = new LogFormatter();
    private int nconnections = 0;
    protected Handler logHandler;

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

        try {
            final java.util.logging.Logger rootlogger = java.util.logging.Logger.getLogger("bookkeeprlite");
            logHandler = new FileHandler(logfile);
            logHandler.setFormatter(logFormatter);
            rootlogger.addHandler(logHandler);
        } catch (IOException ex) {
            logHandler = null;
        }
        logger.info("Starting bookkeeprlite");
        // load the SQLite driver.
        try {
            logger.debug("Loading sqlite driver");
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            logger.error("sqlite driver not found on classpath.", ex);
            throw new BookKeeprLiteException(ex);
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
        logger.debug("Testing the database connection");
        // Try and connect to the database.
        try {
            conn = this.connectDB();

            conn.setAutoCommit(false);

            Statement s = conn.createStatement();

            synchronized (this) {
                logger.debug("Trying to drop 'test' table");
                s.executeUpdate("drop table if exists `test`;");

                logger.debug("Trying to create 'test' table");
                s.executeUpdate("create table `test` (key,val);");

                conn.commit();
            }
            logger.debug("Trying to insert test rows");
            for (int i = 0; i < 100; i++) {
                s.addBatch("insert into `test` values ('k" + i + "','v" + i + "');");
            }

            synchronized (this) {
                s.executeBatch();
                conn.commit();
            }

            synchronized (this) {

                logger.debug("Trying to select test rows");
                ResultSet rs = s.executeQuery("select * from `test`;");

                int i = 0;
                while (rs.next()) {
                    if (!rs.getString("key").equals("k" + i)) {
                        rs.close();
                        this.closeDB(conn);
                        throw new BookKeeprLiteException("Error, could not get the correct keys back from the dabase");
                    }
                    i++;
                }
                rs.close();

                logger.debug("Trying to drop test table");
                s.executeUpdate("drop table if exists `test`;");
                conn.commit();

            }

            synchronized (this) {

                logger.debug("Testing for pointings table");
                ResultSet rs = s.executeQuery("select count(*) from `pointings`;");
                int nptg = -1;
                if (rs.next()) {
                    nptg = rs.getInt(1);
                }
                rs.close();
                logger.info("Found {} pointings in the database", nptg);
            }

            synchronized (this) {

                logger.debug("Testing for beams table");
                ResultSet rs = s.executeQuery("select count(*) from `beams`;");
                int nptg = -1;
                if (rs.next()) {
                    nptg = rs.getInt(1);
                }
                rs.close();
                logger.info("Found {} beams in the database", nptg);
            }

            synchronized (this) {

                logger.debug("Testing for psrxml table");
                ResultSet rs = s.executeQuery("select count(*) from `psrxml`;");
                int n = -1;
                if (rs.next()) {
                    n = rs.getInt(1);
                }
                rs.close();
                logger.info("Found {} psrxml entries in the database", n);
            }

            this.closeDB(conn);

        } catch (SQLException ex) {
            this.closeDB(conn);

            throw new BookKeeprLiteException("An error occured trying to test the dabase", ex);
        }

        logger.info("Test shows database can be read and written ok");

    }

    /**
     * Initiaises an empty database. Warning: Calling this method will erase any
     * existing database!
     *
     * @throws BookKeeprLiteException on a database failure.
     */
    public void initialiseBookKeepr() throws BookKeeprLiteException {
        logger.info("Initialising the database, this will remove all entries!");

        Connection conn = null;
        // Try and connect to the database.
        try {
            conn = this.connectDB();

            conn.setAutoCommit(false);

            Statement s = conn.createStatement();
            synchronized (this) {
                logger.debug("Re-creating pointings table");

                s.executeUpdate("drop table if exists `pointings`;");
                s.executeUpdate("create table `pointings` (" +
                        "`uid` integer primary key,`gridid`, `coordinate` text, " +
                        "`rise` real, `set` real, `toobserve` text, `survey` text ," +
                        " `region` text, `tobs` real, `config` text,`ra` real, " +
                        "`dec` real,`gl` real,`gb` real);");

                conn.commit();

                logger.debug("Re-creating survey beams table");

                s.executeUpdate("drop table if exists `beams`;");
                s.executeUpdate("create table `beams` (" +
                        "`uid` integer primary key,`pointing_uid` integer," +
                        " `coordinate` text, `ra` real,`dec` real,`gl` real,`gb` real,`skybeam` integer, " +
                        "  foreign key(pointing_uid) references pointing(uid));");

                conn.commit();

                logger.debug("Re-creating psrxml table");

                s.executeUpdate("drop table if exists `psrxml`;");
                s.executeUpdate("create table `psrxml` (`uid` integer primary key, " +
                        "`beam_uid` integer, `url` text, `coordinate` text ," +
                        "`ra` real ,`dec` real,`gl` real ,`gb` real ,`utcstart` text," +
                        "`lst`,`beam` integer,`tobs` real,`source_id` text ,`programme` text," +
                        " foreign key(beam_uid) references beams(uid));");

                conn.commit();
            }
            this.closeDB(conn);
        } catch (SQLException ex) {
            throw new BookKeeprLiteException("An error occured trying to initialise the database", ex);
        }
        logger.info("Database initialisation complete");
    }

    /**
     *
     * Connects to the SQLite database.
     *
     * @return A connection to the database.
     * @throws java.sql.SQLException
     */
    public Connection connectDB() throws SQLException {

        logger.debug("A new database connection was requested");
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbname);

        org.sqlite.Function.create(conn, "sepnGal", new GalacticDistanceComparatorDBFunction());
        nconnections++;
        return conn;
    }

    public void closeDB(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                nconnections--;
            } catch (SQLException ex) {
                logger.debug("Error closing db connection", ex);
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        java.util.logging.Logger rootlogger = java.util.logging.Logger.getLogger("");
        for (Handler h : rootlogger.getHandlers()) {
            rootlogger.removeHandler(h);
        }
        Handler h = new ConsoleHandler();
        LogFormatter lf = new LogFormatter();
        h.setFormatter(lf);
        rootlogger.addHandler(h);

        final Logger logger = LoggerFactory.getLogger(BookKeeprLite.class);

        boolean quiet = false;
        boolean interactive = false;
        boolean reinit = false;
        boolean server = false;
        boolean longlog = false;
        int port = 22000;



        for (String a : args) {
            String shortargs = "";
            if (a.startsWith("-") && a.charAt(1) != '-') {
                shortargs = a;
            }


            if (a.equals("--verbose") || shortargs.contains("V")) {
                java.util.logging.Logger.getLogger("bookkeeprlite").setLevel(Level.ALL);
                h.setLevel(Level.ALL);
                logger.info("Verbose logging enabled.");
            }

            if (a.equals("--long-log")) {
                longlog = true;
                lf.setClasses(true);
                logger.info("Extra log info requested.");
            }

            if (a.equals("--jetty-verbose")) {
                java.util.logging.Logger.getLogger("org.mortbay").setLevel(Level.ALL);
                logger.info("Verbose jetty logging enabled.");
            }

            if (a.startsWith("--port=")) {
                String[] elems = a.split("=");
                port = Integer.parseInt(elems[1]);
            }

            if (a.equals("--interactive") || shortargs.contains("i")) {
                interactive = true;
            }

            if (a.equals("--server") || shortargs.contains("s")) {
                server = true;
            }

            if (a.equals("--quiet") || shortargs.contains("q")) {
                quiet = true;
            }
        }

        if (!interactive) {
            try {
                System.in.close();
            } catch (IOException ex) {
            }
        }

        try {
            logger.info("Attempt to start a bookkeepr instance");
            BookKeeprLite bk = new BookKeeprLite("test.db", "bookkeepr.log");
            bk.logFormatter.setClasses(longlog);
            logger.info("bookkeepr instance started ok");


            if (reinit) {
                logger.warn("re-initialising the database: This removes all contents!!");
                bk.initialiseBookKeepr();
            }

            if (quiet) {
                logger.info("quiet mode, closing output");
                System.out.close();
                System.err.close();
            }


            if (interactive) {
                InteractiveConsole.interactiveConsole(bk);
            } else {
                bk.test();
            }

            if (server) {
                new BookKeeprServer(bk, 22000).start();
            }

        } catch (BookKeeprLiteException ex) {
            logger.error("An exception has occured whilst running the bookkeepr", ex);
        }

    }
}
