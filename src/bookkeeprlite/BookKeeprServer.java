/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.mortbay.jetty.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mkeith
 */
public class BookKeeprServer {

    private final Logger logger = LoggerFactory.getLogger(BookKeeprLite.class);
    private Server jetty;
    private BookKeeprLite bk;

    public BookKeeprServer(BookKeeprLite bk, int port) {
        this.bk = bk;
        // Set up logging, so that jetty messages are logged to log file.
        java.util.logging.Logger.getLogger("org.mortbay").addHandler(bk.logHandler);

        logger.debug("Creating jetty instance");
        jetty = new Server(port);
        WebJettyHandler wjh = new WebJettyHandler("./web");
        jetty.addHandler(new ObservationJettyHandler(bk, wjh));
        jetty.addHandler(wjh);
    }

    public void start() {
        try {
            logger.info("Starting the jetty http server");
            jetty.start();
        } catch (Exception ex) {
            logger.error("An error occured in the jetty server", ex);
        }
    }

    static Map<String, String> splitHttpForm(String line) {
        if (line == null) {
            return null;
        }
        String[] parts = line.split("&");
        Map<String, String> res = new HashMap<String, String>();
        for (String s : parts) {
            String[] kv = s.split("=");
            if (kv.length < 2) {
                continue;
            }
            try {

                res.put(kv[0], URLDecoder.decode(kv[1], "UTF-8"));
            } catch (UnsupportedEncodingException ex) {
                LoggerFactory.getLogger(BookKeeprLite.class).error("Your java does not support UTF-8 decoding, a little difficult to contiune.");
            }
        }
        return res;
    }

    static double getDoubleFromMap(Map<String, String> requestMap, String key) {
        double x = Double.NaN;
        if (requestMap.get(key) != null) {
            try {
                x = Double.parseDouble(requestMap.get("coord_theta"));
            } catch (NumberFormatException e) {
                x = Double.NaN;
            }
        }
        return x;
    }

    static int getIntFromMap(Map<String, String> requestMap, String key) {
        int x = -1;
        if (requestMap.get(key) != null) {
            try {
                x = Integer.parseInt(requestMap.get("coord_theta"));
            } catch (NumberFormatException e) {
                x = -1;
            }
        }
        return x;
    }

    static boolean getBooleanFromMap(Map<String, String> requestMap, String key) {
        return (requestMap.get(key) != null);
    }
}
