/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package bookkeeprlite;

/**
 * A wrapper exception for the various errors that might occur
 * when running bookkeeprlite.
 *
 * @author kei041
 */
public class BookKeeprLiteException extends Exception{

    public BookKeeprLiteException(Throwable cause) {
        super(cause);
    }

    public BookKeeprLiteException(String message, Throwable cause) {
        super(message, cause);
    }

    public BookKeeprLiteException(String message) {
        super(message);
    }

    public BookKeeprLiteException() {
    }


}
