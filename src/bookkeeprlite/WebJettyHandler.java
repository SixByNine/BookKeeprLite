/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mkeith
 */
public class WebJettyHandler extends AbstractHandler {

    final String rootpath;
    private static Pattern badchar = Pattern.compile("\\.\\.");
    private final Logger logger = LoggerFactory.getLogger(WebJettyHandler.class);

    public WebJettyHandler(String rootpath) {
        this.rootpath = rootpath;
    }

    public void handle(String path, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        if (!((Request) request).isHandled()) {
            logger.debug("WJH Handling request for '" + path + "'");
            ((Request) request).setHandled(true);
            // just a check that the path is a subpath of the website
            if (badchar.matcher(path).matches()) {
                response.sendError(400, "User Error");
                return;
            }
            if (writeWebOut(rootpath + File.separator + path, request, response)) {
                response.getOutputStream().close();
            }
        }
    }

    /**
     * Writes the file specified by "path" to the http response object
     * If all goes well it will return true. If it had to send a 404 etc,
     * then it will return false and the response should be considered sent.
     *
     * @param path The path to the local file to send.
     * @param request The http request used (for transmision headers)
     * @param response The http response to send to
     * @throws IOException If the file is not found
     * @throws ServletException If there is an error in the http transimssion.
     * @return true if the response can still be used.
     */
    public boolean writeWebOut(String path, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        File targetFile = new File(path);
        logger.debug("Trying to send " + targetFile.getAbsolutePath());
        // Check if the target is a dir
        if (targetFile.isDirectory()) {
            if (path.endsWith("/")) {
                // send index.html
                targetFile = new File(path + "index.html");
                logger.debug("Trying index.html");
                if (!targetFile.exists()) {
                    targetFile = new File(path + "index.xhtml");
                    logger.debug("Trying index.xhtml");
                }
            } else {
                // else redirect to the correct url.
                response.sendRedirect(path + "/");
                logger.debug("Redirecting to / terminated directory");
                return false;
            }
        }
        if (targetFile.exists()) {

            PrintStream out = new PrintStream(response.getOutputStream());



            if (targetFile.getName().endsWith(".xhtml") || targetFile.getName().endsWith(".html") || targetFile.getName().endsWith(".xsl")) {


                BufferedReader in = new BufferedReader(new FileReader(targetFile));

                String line = in.readLine();
                while (line != null) {
                    if (line.trim().startsWith("%%%")) {
                        logger.debug("Sending included file " + line.trim().substring(3));
                        BufferedReader wrapper = new BufferedReader(new FileReader(rootpath + "/inc/" + line.trim().substring(3) + ".html"));
                        String line2 = wrapper.readLine();
                        while (line2 != null) {
                            out.println(line2);
                            line2 = wrapper.readLine();
                        }
                        wrapper.close();
                    } else {
                        out.println(line);
                    }
                    line = in.readLine();
                }
                in.close();
                out.flush();
                return true;
            } else {
                outputToInput(new FileInputStream(targetFile), out);
                return true;
            }
        } else {
            logger.debug("Could not find requested file, sending 404!");
            response.sendError(response.SC_NOT_FOUND);
            return false;
        }

    }

    private void outputToInput(InputStream in, OutputStream out) throws FileNotFoundException, IOException, IOException {

        byte[] b = new byte[1024];
        while (true) {
            int count = in.read(b);
            if (count < 0) {
                break;
            }
            out.write(b, 0, count);
        }
        in.close();
        out.flush();
    }
}
