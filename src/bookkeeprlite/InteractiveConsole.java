/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.sql.Statement;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic interactive console for bookkeeprlite
 *
 * This allows for users to type in SQL instructions to the database.
 *
 * @author kei041
 */
public class InteractiveConsole {

    static void interactiveConsole(BookKeeprLite bk) {
        try {
            Connection c = bk.connectDB();

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("bookkeepr> ");
            System.out.flush();
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith("!q")) {
                    break;
                }
                if (line.startsWith("!e")) {
                    break;
                }

                if (line.startsWith("!reinit")) {

                    System.out.print("This will DELETE the database\nARE YOU SURE? (y/n)");
                    System.out.flush();
                    line = reader.readLine();
                    if (line.equalsIgnoreCase("y")) {
                        bk.initialiseBookKeepr();
                    }
                    System.out.println("Initialisation complete.");
                    System.out.print("bookkeepr> ");

                    line = reader.readLine();
                    continue;

                }
                if (line.startsWith("!test")) {
                    try {
                        bk.test();
                    } catch (BookKeeprLiteException ex) {
                        ex.printStackTrace();
                    }
                    System.out.print("bookkeepr> ");
                    System.out.flush();
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("!begin")) {
                    c.setAutoCommit(false);
                    System.out.print("bookkeepr> ");
                    System.out.flush();
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("!commit")) {
                    c.commit();
                    c.setAutoCommit(true);
                    System.out.print("bookkeepr> ");
                    System.out.flush();
                    line = reader.readLine();
                    continue;
                }
                if (line.startsWith("!rollback")) {
                    c.rollback();
                    c.setAutoCommit(true);
                    System.out.print("bookkeepr> ");
                    System.out.flush();
                    line = reader.readLine();
                    continue;
                }

                synchronized (bk) {
                    try {
                        Statement smt = c.createStatement();
                        if (smt.execute(line)) {

                            ResultSet rs = smt.getResultSet();

                            ResultSetMetaData md = rs.getMetaData();
                            int ncol = md.getColumnCount();
                            for (int i = 1; i <= ncol; i++) {
                                System.out.printf("%8s ", md.getColumnName(i));
                            }
                            System.out.println("\n==");

                            while (rs.next()) {
                                for (int i = 1; i <= ncol; i++) {
                                    System.out.printf("%8s ", rs.getString(i));
                                }
                                System.out.println();
                            }
                        } else {
                            System.out.println(smt.getUpdateCount() + " lines updated");
                        }
                    } catch (SQLException ex) {
                        System.out.println(ex.getLocalizedMessage());
                    }
                }
                System.out.print("bookkeepr> ");
                System.out.flush();
                line = reader.readLine();
            }

bk.closeDB(c);

        } catch (Exception exception) {
            Logger.getLogger(InteractiveConsole.class.getName()).log(Level.SEVERE, exception.getMessage(), exception);
        }
    }
}
