/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package bookkeeprlite.datastructures;

import coordlib.Coordinate;

/**
 *
 * @author kei041
 */
public class SurveyBeam {
// "`uid` integer primary key,`pointing_uid` integer," +
//                        " `coordinate` text, `ra` real,`dec` real,`gl` real,`gb` real,`skybeam` integer, " +
//                        "  foreign key(pointing_uid) references pointing(uid));");
    private long key;
    private long pointingKey;
    private String coordStr;
    private Coordinate coordinate=null;
    private int skybeam;

    public SurveyBeam() {
    }

    public SurveyBeam(long key, long pointingKey, String coordStr, int skybeam) {
        this.key = key;
        this.pointingKey = pointingKey;
        this.coordStr = coordStr;
        this.skybeam = skybeam;
    }



    public String getCoordStr() {
        return coordStr;
    }

    public void setCoordStr(String coordStr) {
        this.coordStr = coordStr;
        this.coordinate=null;
    }

    public long getPointingKey() {
        return pointingKey;
    }

    public void setPointingKey(long pointingKey) {
        this.pointingKey = pointingKey;
    }


    



    public Coordinate getCoordinate() {
        if(coordinate==null)coordinate = new Coordinate(coordStr);
        return coordinate;
    }

    public void setCoordinate(Coordinate coordinate) {
        this.coordinate = coordinate;
        this.coordStr = coordinate.toString();
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int getSkybeam() {
        return skybeam;
    }

    public void setSkybeam(int skybeam) {
        this.skybeam = skybeam;
    }



}
