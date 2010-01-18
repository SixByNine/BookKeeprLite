/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package bookkeeprlite.sqlitefunctions;

import java.sql.SQLException;
import org.sqlite.Function;

/**
 *
 * @author mkeith
 */
public class GalacticDistanceComparatorDBFunction extends Function {

    @Override
    protected void xFunc() throws SQLException {
        
        double gl1 = this.value_double(0);
        double gb1 = this.value_double(1);
        double gl2 = this.value_double(2);
        double gb2 = this.value_double(3);

        this.result(new coordlib.CoordinateDistanceComparitorGalactic(gl1, gb1).difference(gl1, gb1, gl2, gb2));

    }




}
