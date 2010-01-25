/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import bookkeepr.xmlable.Psrxml;
import bookkeeprlite.datastructures.SurveyBeam;
import bookkeeprlite.datastructures.SurveyPointing;
import coordlib.Coordinate;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * This class handles the http requests that are related to observing and
 * managing of observations.
 *
 * @author mkeith
 */
public class ObservationJettyHandler extends AbstractHandler {

    private final Logger logger = LoggerFactory.getLogger(ObservationJettyHandler.class);
    private final BookKeeprLite bk;
    private final WebJettyHandler wjh;

    public ObservationJettyHandler(BookKeeprLite bk, WebJettyHandler wjh) {
        this.bk = bk;
        this.wjh = wjh;
    }

    public void handle(String path, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {

        if (path.startsWith("/obs/search")) {
            handleSearchRequest(path, request, response, dispatch);
        }
    }

    public void handleStorePsrxmlRequest(String path, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        ((Request) request).setHandled(true);

        Map<String, String> requestMap = null;
        Psrxml psrxml = null;


        if (request.getMethod().equals("POST")) {

            logger.info("Handling a request to store a psrxml file.");
            try {
                psrxml = (Psrxml) bookkeepr.xml.XMLReader.read(request.getInputStream());
            } catch (Exception ex) {
                logger.error("Bad psrxml file passed!", ex);
            }


        } else {
            response.sendError(400, "Must be a post request.");
            logger.debug("Got a non-POST request for psrxml handling...?");
            return;
        }

        if (psrxml == null) {
            response.sendError(400, "Could not parse given PSRXML data.");
            return;
        }

        Connection c = null;
        try {
            c = bk.connectDB();
            c.setAutoCommit(false);

            String gridid = psrxml.getSourceNameCentreBeam();
            String project = psrxml.getObservingProgramme();

            PreparedStatement s = c.prepareStatement("select `id` from `pointings` where gridid=? and survey=? limit 1;");
            s.setString(1, gridid);
            s.setString(2, project);
            long ptg_id = -1;
            long beam_id = -1;
            Coordinate coord = psrxml.getStartCoordinate().getCoordinate();

            synchronized (bk) {
                ResultSet rs = s.executeQuery();

                if (rs.next()) {
                    ptg_id = rs.getLong(1);
                }
                rs.close();
                s.close();

                s = c.prepareStatement("select `id` from `beams` where pointing_uid=? and sepnGal(?,?,gl,gb) order by sepnGal(?,?,gl,gb) limit 1;");
                s.setLong(1, ptg_id);
                s.setDouble(2, coord.getGl());
                s.setDouble(3, coord.getGb());
                s.setDouble(4, coord.getGl());
                s.setDouble(5, coord.getGb());
                rs = s.executeQuery();
                if (rs.next()) {
                    beam_id = rs.getLong(1);
                }
                rs.close();
                s.close();

            }




            /*s.executeUpdate("create table `psrxml` (`uid` integer primary key, " +
            "`beam_uid` integer, `url` text, `coordinate` text ," +
            "`ra` real ,`dec` real,`gl` real ,`gb` real ,`utcstart` text," +
            "`lst`,`beam` integer,`tobs` real,`source_id` text ,`programme` text," +
            " foreign key(beam_uid) references beams(uid));");*/


            int i = 0;
            if (beam_id < 0) {
                s = c.prepareStatement("insert into `psrxml` values (null,null,?,?,?,?,?,?,?,?)");
            } else {
                s = c.prepareStatement("insert into `psrxml` values (null,?,?,?,?,?,?,?,?,?)");
                s.setLong(++i, beam_id);
            }

            s.setString(++i, psrxml.getCatReference());
            s.setString(++i, coord.toString(false));
            s.setDouble(++i, coord.getRA().toDegrees());
            s.setDouble(++i, coord.getDec().toDegrees());
            s.setDouble(++i, coord.getGl());
            s.setDouble(++i, coord.getGb());
            s.setString(++i, psrxml.getUtc());
            s.setString(++i, psrxml.getLst());
            s.setInt(++i, psrxml.getReceiverBeam());
            s.setDouble(++i, psrxml.getActualObsTime());
            s.setString(++i, psrxml.getSourceName());
            s.setString(++i, psrxml.getObservingProgramme());
            synchronized (bk) {

                int nrows = s.executeUpdate();
                s.close();
                if (nrows != 1) {
                    logger.error("Update of psrxml did affected {} rows (should be 1)", nrows);
                    response.sendError(500, "error occured inserting psrxml in database.");
                    return;
                }

                // update pointings.

                if (ptg_id >= 0 && beam_id >= 0) {
                    s = c.prepareStatement("update `pointings` set (toobserve == false) where uid=? limit 1 ; ");
                    s.setLong(++i, ptg_id);
                    s.executeUpdate();
                    s.close();
                }

                c.commit();
            }
            bk.closeDB(c);
        } catch (SQLException ex) {
            bk.closeDB(c);
            logger.error("Exception occured storing psrxml in database", ex);
        }
    }

    /**
     * Handles http requests to search the observations database, using http
     * POST or GET.
     *
     * @param path
     * @param request
     * @param response
     * @param dispatch
     * @throws IOException
     * @throws ServletException
     */
    public void handleSearchRequest(String path, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        ((Request) request).setHandled(true);
        logger.info("Handling a database search request.");


        Map<String, String> requestMap = null;
        /*
         * Try and parse the query string, depending if it's a POST or GET
         * request. Other requests will leave the query as null and therefore
         * send the search page.
         *
         */
        if (request.getMethod().equals("POST")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()));
            String line = reader.readLine();
            if (line != null) {
                requestMap = BookKeeprServer.splitHttpForm(line);
            } else {
                logger.info("Received a search POST request with no contents!");
            }
        }
        if (request.getMethod().equals("GET")) {
            requestMap = BookKeeprServer.splitHttpForm(request.getQueryString());
        }
        if (requestMap == null) {
            if (wjh.writeWebOut(wjh.rootpath + "/obs/search/index.html", request, response)) {
                response.getOutputStream().close();
            }
            return;
        }


        /*
         * Get the search parameters from the http request
         */
        String gridid = requestMap.get("gridid");
        if (gridid != null && gridid.trim().equals("")) {
            gridid = null;
        }
        String obstype = requestMap.get("obstype");
        if (obstype == null) {
            obstype = "both";
        }

        double distmax = BookKeeprServer.getDoubleFromMap(requestMap, "distmax");
        String coord_str = requestMap.get("coord");
        if (coord_str == null) {
            coord_str = "G 0 0";
        }
        Coordinate coord = new Coordinate(coord_str);
        double gl = coord.getGl();
        double gb = coord.getGb();

        int nmax = BookKeeprServer.getIntFromMap(requestMap, "nmax");

        if (nmax < 0 || nmax > 1000) {
            nmax = 1000;
        }


        logger.debug("Search gridid={} obstype={} distmax={} coord={} nmax={}", new Object[]{gridid, obstype, distmax, coord, nmax});


        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/header.html", request, response)) {
            return;
        }


        /*
         * Here we create the bulk of the "where" clause, depending on what
         * search options were used.
         */
        StringBuffer whereClause = new StringBuffer();

        if (gridid != null) {
            whereClause.append(" AND `gridid` == ?");

        }

        if (obstype.equals("toobserve")) {
            whereClause.append(" AND `toobserve` == 'true'");
        }
        if (obstype.equals("nottoobserve")) {
            whereClause.append(" AND `toobserve` == 'false'");
        }

        /*
         * These arraylists are used later on.
         */
        ArrayList<String> uids = new ArrayList<String>();
        ArrayList<Double> sepns = null; // this will be null if there was no coord.
        ArrayList<SurveyPointing> pointings = new ArrayList<SurveyPointing>();




        // Begin talking to the database... open a connection
        Connection conn = null;
        try {
            conn = bk.connectDB();


            /*
             * We have two cases:
             *
             * 1) We are searching by position, then we need to select beams first.
             * 2) We are searching by some other criteria, no need to select beams.
             *
             */
            if (distmax > 0) {
                /**
                 * We compute the "sepn" field for all database points that lie
                 * in a sensible Gb region. This makes things faster later on.
                 *
                 */
                {
                    conn.setAutoCommit(false);
                    PreparedStatement s = conn.prepareStatement("select uid, " +
                            "pointing_uid, sepnGal(?,?,beams.gl,beams.gb) " +
                            " from beams where gb < ? and gb > ?;");


                    //select
                    s.setDouble(1, gl);
                    s.setDouble(2, gb);

                    // where
                    s.setDouble(3, gb + distmax);
                    s.setDouble(4, gb - distmax);

                    ResultSet rs = s.executeQuery();

                    Statement ss = conn.createStatement();
                    ss.execute("create temporary table beamsearch (uid integer primary key, pointing_uid integer, sepn double)");
                    ss.close();

                    PreparedStatement s2 = conn.prepareStatement("insert into beamsearch values (?,?,?)");
                    int i = 0;
                    while (rs.next()) {
                        s2.setLong(1, rs.getLong(1));
                        s2.setLong(2, rs.getLong(2));
                        s2.setDouble(3, rs.getDouble(3));
                        s2.addBatch();
                        i++;
                    }
                    logger.debug("Found {} beams for temp table", i);
                    // Make sure to close the resultset and the statements
                    // to free database resources.
                    rs.close();
                    s.close();

                    // execute the insert statements, then close.
                    s2.executeBatch();
                    conn.commit();
                    s2.close();


                }

                StringBuffer query = new StringBuffer("select pointings.uid," +
                        " sepn, gridid from `beamsearch` inner join `pointings`" +
                        " on  pointings.uid = beamsearch.pointing_uid" +
                        " where sepn < ?");

                query.append(whereClause);
                query.append(" order by sepn limit ");
                query.append(nmax * 13);
                query.append(";");
                sepns = new ArrayList<Double>();

                synchronized (bk) {

                    int i = 0;
                    PreparedStatement s = conn.prepareStatement(query.toString());

                    // Here we are setting the variables for the statement


                    //where
                    s.setDouble(++i, distmax);

                    if (gridid != null) {
                        s.setString(++i, gridid);
                    }




                    // Query is open DATABASE LOCKED!
                    ResultSet rs = s.executeQuery();
                    while (rs.next()) {
                        uids.add(rs.getString(1));
                        sepns.add(rs.getDouble(2));
                        logger.debug("Found nearby ptg {}", rs.getString(3));
                    }
                    // Resultset closed, released database
                    rs.close();
                    s.close();
                }



            } else {
                StringBuffer query = new StringBuffer("select `uid` from" +
                        "`pointings` where 1");
                query.append(whereClause);
                query.append(" limit ");
                query.append(nmax * 13);
                query.append(";");

                int i = 0;
                PreparedStatement s = conn.prepareStatement(query.toString());

                // Here we are setting the variables for the statement
                if (gridid != null) {
                    s.setString(++i, gridid);
                }

                // Query is open DATABASE LOCKED!
                ResultSet rs = s.executeQuery();
                while (rs.next()) {
                    uids.add(rs.getString(1));
                }
                // Resultset closed, released database
                rs.close();
                s.close();
            }

            // Now get the pointings.
            StringBuffer query = new StringBuffer("select `uid`, `gridid`, `survey`, `region`," +
                    " `coordinate`, `tobs`, `toobserve`" +
                    "from pointings where `uid` = ?");

            query.append(whereClause);
            query.append("limit 1;");

            synchronized (bk) {
                PreparedStatement s = conn.prepareStatement(query.toString());
                int n = 0;
                for (String uid : uids) {
                    // Here we are setting the variables for the statement
                    int i = 0;

                    s.setString(++i, uid);

                    if (gridid != null) {
                        s.setString(++i, gridid);
                    }

                    ResultSet rs = s.executeQuery();
                    SurveyPointing ptg = new SurveyPointing();
                    if (rs.next()) {
                        ptg = new SurveyPointing();
                        ptg.setUid(rs.getLong(1));
                        ptg.setGridId(rs.getString(2));
                        ptg.setSurvey(rs.getString(3));
                        ptg.setRegion(rs.getString(4));
                        ptg.setCoordStr(rs.getString(5));
                        ptg.setTobs(rs.getDouble(6));
                        ptg.setToObserve(rs.getString(7));

                        ptg.setBeams(new ArrayList<SurveyBeam>());
                    }
                    rs.close();

                    pointings.add(ptg);
                    n++;
                    if (n > nmax) {
                        break;
                    }
                }
                s.close();
            }
            bk.closeDB(conn);
        } catch (SQLException ex) {
            // An exception has happened, but make sure to free the database
            // connection
            bk.closeDB(conn);
            logger.error("An exception occured trying to talk to the database.", ex);
        }


        // Now write results to user...
        PrintStream out = new PrintStream(response.getOutputStream());
        int i = 0;
        out.println("<tr>");
        if (sepns != null) {
            out.printf("<th>Sepn</th>");
        }
        out.printf("<th>GridID</th><th>Coord</th><th>T<sub>obs</sub></th>");
        out.println("</tr>");


        for (SurveyPointing ptg : pointings) {
            out.println("<tr>");
            if (sepns != null) {
                out.printf("<td>%f</td>", sepns.get(i));
            }
            out.printf("<td>%s</td><td>%s</td><td>%5.0f</td>", ptg.getGridId(), ptg.getCoordinate().toString(false), ptg.getTobs());
            out.println("</tr>");
            i++;
        }

        out.flush();

        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/footer.html", request, response)) {
            return;
        }
        out.close();
    }
}
