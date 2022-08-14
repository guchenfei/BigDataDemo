package flink.table;

import java.io.Serializable;

public class PlayerData implements Serializable {
    private static final long serialVersionUID = -1827416818754578232L;

    public String season;
    public String player;
    public String playNum;
    public Integer firstCourt;
    public Double time;
    public Double assists;
    public Double steals;
    public Double blocks;
    public Double scores;

    public PlayerData() {
    }

    public PlayerData(String season, String player, String playNum, Integer firstCourt, Double time, Double assists, Double steals, Double blocks, Double scores) {
        this.season = season;
        this.player = player;
        this.playNum = playNum;
        this.firstCourt = firstCourt;
        this.time = time;
        this.assists = assists;
        this.steals = steals;
        this.blocks = blocks;
        this.scores = scores;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }

    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public String getPlayNum() {
        return playNum;
    }

    public void setPlayNum(String playNum) {
        this.playNum = playNum;
    }

    public Integer getFirstCourt() {
        return firstCourt;
    }

    public void setFirstCourt(Integer firstCourt) {
        this.firstCourt = firstCourt;
    }

    public Double getTime() {
        return time;
    }

    public void setTime(Double time) {
        this.time = time;
    }

    public Double getAssists() {
        return assists;
    }

    public void setAssists(Double assists) {
        this.assists = assists;
    }

    public Double getSteals() {
        return steals;
    }

    public void setSteals(Double steals) {
        this.steals = steals;
    }

    public Double getBlocks() {
        return blocks;
    }

    public void setBlocks(Double blocks) {
        this.blocks = blocks;
    }

    public Double getScores() {
        return scores;
    }

    public void setScores(Double scores) {
        this.scores = scores;
    }

    @Override
    public String toString() {
        return "PlayerData{" +
                "season='" + season + '\'' +
                ", player='" + player + '\'' +
                ", playNum='" + playNum + '\'' +
                ", firstCourt=" + firstCourt +
                ", time=" + time +
                ", assists=" + assists +
                ", steals=" + steals +
                ", blocks=" + blocks +
                ", scores=" + scores +
                '}';
    }
}
