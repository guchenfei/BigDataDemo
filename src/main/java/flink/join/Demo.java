package flink.join;

import com.alibaba.fastjson.JSON;

public class Demo {
    public static void main(String[] args) {
        UserInfo userInfo = new UserInfo();
        userInfo.setUserName("google");
        userInfo.setCityId(1000);
        userInfo.setTs(1L);

//        CityInfo cityInfo = new CityInfo();
//        cityInfo.setCityId(1000);
//        cityInfo.setCityName("北京");
//        cityInfo.setTs(1L);

        System.out.println(JSON.toJSONString(userInfo));
    }
}
