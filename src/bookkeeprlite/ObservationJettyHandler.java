/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


        StringBuffer whereClause = new StringBuffer();

        if (gridid != null) {
            whereClause.append(" AND pointings.gridid == ?");

        }


        if (obstype.equals("toobserve")) {
            whereClause.append(" AND pointings.toobserve == 'true'");
        }
        if (obstype.equals("nottoobserve")) {
            whereClause.append(" AND toobserve == 'false'");
        }

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
                 * @TODO: Make this faster by splitting into regions and/or
                 * making virtual tables.
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
                    rs.close();
                    s.close();
                    i = 0;
                    for (int x : s2.executeBatch()) {
                        i += x;
                    }
                    logger.debug("Inserted {} rows in temp table", i);
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
                    "from pointings where `uid` = ? limit 1");

            query.append(whereClause);
            query.append(";");

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
