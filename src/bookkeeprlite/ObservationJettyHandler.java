/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

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
            if (wjh.writeWebOut(wjh.rootpath + "/obs/search/index.xhtml", request, response)) {
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
            nmax = 10;
        }


        logger.debug("Search gridid={} obstype={} distmax={} coord={} nmax={}",new Object[]{gridid,obstype,distmax,coord,nmax});


        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/header.xhtml", request, response)) {
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

        PrintStream out = new PrintStream(response.getOutputStream());



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
                StringBuffer query = new StringBuffer("select pointings.uid," +
                        " sepnGal(?,?,beams.gl,beams.gb), gridid from `beams` inner join `pointings`" +
                        " on  pointings.uid = beams.pointing_uid" +
                        " where sepnGal(?,?,beams.gl,beams.gb) < ?");

                query.append(whereClause);
                query.append(" order by sepnGal(?,?,beams.gl,beams.gb) limit ");
                query.append(nmax * 13);
                query.append(";");
                sepns = new ArrayList<Double>();

                synchronized (bk) {

                    int i = 0;
                    PreparedStatement s = conn.prepareStatement(query.toString());

                    // Here we are setting the variables for the statement

                    //select
                    s.setDouble(++i, gl);
                    s.setDouble(++i, gb);

                    //where
                    s.setDouble(++i, gl);
                    s.setDouble(++i, gb);
                    s.setDouble(++i, distmax);

                    if (gridid != null) {
                        s.setString(++i, gridid);
                    }

                    //order by
                    s.setDouble(++i, gl);
                    s.setDouble(++i, gb);



                    // Query is open DATABASE LOCKED!
                    ResultSet rs = s.executeQuery();
                    while (rs.next()) {
                        uids.add(rs.getString(1));
                        sepns.add(rs.getDouble(2));
                        logger.debug("Found nearby ptg {}",rs.getString(3));
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
            StringBuffer query = new StringBuffer("select pointings.uid, " +
                    "pointings.gridid, beams.coordinate, pointings.toobserve, " +
                    "pointings.survey , pointings.region, pointings.tobs, " +
                    " pointings.config" +
                    " from beams inner join pointings " +
                    " on pointings.uid = beams.pointing_uid" +
                    " where pointings.uid = ?");

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
                    // todo send data to user!
                    out.println(rs.getString(2)+" "+sepns.get(n)+ " ");
                    rs.close();
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

        out.flush();

        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/footer.xhtml", request, response)) {
            return;
        }
        out.close();
    }
}
