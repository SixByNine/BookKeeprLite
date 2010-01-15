/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import javax.swing.text.DateFormatter;

/**
 * Formats the bookkeeprlite logs in a compact, nice way.
 *
 * @author kei041
 */
public class LogFormatter extends Formatter {

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    @Override
    public String format(LogRecord record) {

        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw);
        out.printf("%6s %s %s\n", record.getLevel().toString(), df.format(new Date(record.getMillis())), record.getMessage());
        if(record.getThrown()!=null){
            out.printf("%6s %s %s\n", "","****************",record.getThrown().getMessage());
            record.getThrown().printStackTrace(out);
        }
        return sw.getBuffer().toString();
    }
}
