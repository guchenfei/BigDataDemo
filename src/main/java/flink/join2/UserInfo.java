package flink.join2;

/**
 *  userInfo.setUid(resultSet.getString("uid"));
 *                         userInfo.setName(resultSet.getString("name"));
 *                         userInfo.setAge(resultSet.getInt("age"));
 *                         userInfo.setAddress(resultSet.getString("address"));
 */
public class UserInfo {
    private String uid;
    private String name;
    private int age;
    private String address;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
