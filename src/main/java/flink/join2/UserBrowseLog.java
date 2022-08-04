package flink.join2;

public class UserBrowseLog {
    private String userId;
    private String eventTimeStr;
    private long eventTime;

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventTimeStr() {
        return eventTimeStr;
    }

    public void setEventTimeStr(String eventTimeStr) {
        this.eventTimeStr = eventTimeStr;
    }
}
