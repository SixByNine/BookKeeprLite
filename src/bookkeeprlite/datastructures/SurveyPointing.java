/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bookkeeprlite.datastructures;

import coordlib.Coordinate;
import java.util.ArrayList;

/**
 *
 * @author kei041
 */
public class SurveyPointing {


//                        "`uid` integer primary key,`gridid`, `coordinate` text, " +
//                        "`rise` real, `set` real, `toobserve` text, `survey` text ," +
//                        " `region` text, `tobs` real, `config` text,`ra` real, " +
//                        "`dec` real,`gl` real,`gb` real);");
    private long uid;
    private String gridId;
    private double rise;
    private double set;
    private String toObserve;
    private String survey;
    private String region;
    private double tobs;
    private String config;
    private String coordStr;
    private Coordinate coordinate = null;
    private ArrayList<SurveyBeam> beams = null;

    public SurveyPointing() {
    }

    public SurveyPointing(long uid, String gridId, double rise, double set, String toObserve, String survey, String region, double tobs, String config, String coordStr) {
        this.uid = uid;
        this.gridId = gridId;
        this.rise = rise;
        this.set = set;
        this.toObserve = toObserve;
        this.survey = survey;
        this.region = region;
        this.tobs = tobs;
        this.config = config;
        this.coordStr = coordStr;
    }

    public ArrayList<SurveyBeam> getBeams() {
        return beams;
    }

    public void setBeams(ArrayList<SurveyBeam> beams) {
        this.beams = beams;
    }
    

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public String getCoordStr() {
        return coordStr;
    }

    public void setCoordStr(String coordStr) {
        this.coordStr = coordStr;
        this.coordinate = null;
    }

    public Coordinate getCoordinate() {
        if (coordinate == null) {
            coordinate = new Coordinate(coordStr);
        }
        return coordinate;
    }

    public void setCoordinate(Coordinate coordinate) {
        this.coordinate = coordinate;
        this.coordStr = coordinate.toString();
    }

    public String getGridId() {
        return gridId;
    }

    public void setGridId(String gridId) {
        this.gridId = gridId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public double getRise() {
        return rise;
    }

    public void setRise(double rise) {
        this.rise = rise;
    }

    public double getSet() {
        return set;
    }

    public void setSet(double set) {
        this.set = set;
    }

    public String getSurvey() {
        return survey;
    }

    public void setSurvey(String survey) {
        this.survey = survey;
    }

    public String getToObserve() {
        return toObserve;
    }

    public void setToObserve(String toObserve) {
        this.toObserve = toObserve;
    }

    public double getTobs() {
        return tobs;
    }

    public void setTobs(double tobs) {
        this.tobs = tobs;
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }
}
