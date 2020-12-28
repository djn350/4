package mostpopular;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class User {
    public static class UserInfo implements Serializable {
        private String UserId;
        private String AgeRange;

        public UserInfo(String line) {
            String[] values = line.split(",", -1);
            setUserId(values[0]);
            setAgeRange(values[1]);
        }

        public String getUserId() {
            return UserId;
        }

        public String getAgeRange() {
            return AgeRange;
        }

        public void setAgeRange(String ageRange) {
            AgeRange = ageRange;
        }

        public void setUserId(String userId) {
            UserId = userId;
        }

        public static boolean judgeYoungPeople(String ageRange) {
            return ageRange.compareTo("0") > 0 && ageRange.compareTo("4") < 0;
        }

        public static Map<String, String> getUserInfo(String UserInfoFileName)
                throws IOException {
            Map<String, String> myUserInfo = new HashMap<>();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(UserInfoFileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                List<String> vals = Arrays.asList(line.split(",", -1));
                myUserInfo.put(vals.get(0), vals.get(1));
            }
            bufferedReader.close();
            return myUserInfo;
        }
    }

    public static class UserLog implements Serializable {
        private String UserId;
        private String ItemId;
        private String SellerId;
        private String ActionType;

        public UserLog(String line) {
            String[] values = line.split(",", -1);
            setUserId(values[0]);
            setItemId(values[1]);
            setSellerId(values[3]);
            setActionType(values[values.length - 1]);
        }

        public String getUserId() {
            return UserId;
        }

        public String getActionType() {
            return ActionType;
        }

        public String getItemId() {
            return ItemId;
        }

        public String getSellerId() {
            return SellerId;
        }

        public void setUserId(String userId) {
            UserId = userId;
        }

        public void setActionType(String actionType) {
            ActionType = actionType;
        }

        public void setItemId(String itemId) {
            ItemId = itemId;
        }

        public void setSellerId(String sellerId) {
            SellerId = sellerId;
        }

        public static boolean isPopular(UserLog userLog) {
            return !userLog.getActionType().equals("0");
        }

        public static boolean isYoungPeople(Map<String, String> UserInfoMap,UserLog userLog) {
            return UserInfo.judgeYoungPeople(UserInfoMap.get(userLog.getUserId()));
        }
    }

}
