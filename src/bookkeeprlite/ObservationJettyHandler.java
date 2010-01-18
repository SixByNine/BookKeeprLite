/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
        String obstype = requestMap.get("obstype");
        if (obstype == null) {
            obstype = "both";
        }

        double distmax = BookKeeprServer.getDoubleFromMap(requestMap, "distmax");
        double coord_theta = BookKeeprServer.getDoubleFromMap(requestMap, "coord_theta");
        double coord_phi = BookKeeprServer.getDoubleFromMap(requestMap, "coord_phi");
        boolean galactic = BookKeeprServer.getBooleanFromMap(requestMap, "galactic");

        if (requestMap.get("coord_theta") != null) {
            try {
                coord_theta = Double.parseDouble(requestMap.get("coord_theta"));
            } catch (NumberFormatException e) {
                coord_theta = Double.NaN;
            }
        }


        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/header.xhtml", request, response)) {
            return;
        }

        /*
         * Prepare for database interaction.
         */
        try {
            StringBuffer query = new StringBuffer("select * from `pointings` where 1");

            if (gridid != null) {
                query.append(" AND `gridid` == ?");

            }

            if (obstype.equals("toobserve")) {
                query.append(" AND `toobserve` == 'true'");
            }
            if (obstype.equals("nottoobserve")) {
                query.append(" AND `toobserve` == 'false'");
            }



            if (distmax > 0) {
                if (galactic) {
                    query.append(" AND sepnGal(?,?,`gl`,`gb`) < ?");
                } else {
                    query.append(" AND sepnGal(?,?,`ra`,`dec`) < ?");
                }

            }

            int i = 0;


            query.append(";");
            
            // Begin talking to the database... open a connection
            Connection conn = bk.connectDB();

            synchronized (bk) {

                // Here we are setting the variables for the statement
                PreparedStatement s = conn.prepareStatement(query.toString());
                if (gridid != null) {
                    s.setString(++i, gridid);
                }

                if (distmax > 0) {
                    s.setDouble(++i, coord_theta);
                    s.setDouble(++i, coord_phi);
                    s.setDouble(++i, distmax);
                }

                // Query is open DATABASE LOCKED!
                ResultSet rs = s.executeQuery();

                // Resultset closed, released database
                rs.close();
                conn.close();
            }

        } catch (SQLException ex) {
            logger.error("An exception occured trying to talk to the database.", ex);
        }

        if (!wjh.writeWebOut(wjh.rootpath + "/obs/search/footer.xhtml", request, response)) {
            return;
        }
    }
}
