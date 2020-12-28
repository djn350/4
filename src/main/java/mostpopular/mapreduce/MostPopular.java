package mostpopular.mapreduce;

import java.io.IOException;

public class MostPopular {
    private final static String UserInfoFileName = "./src/main/resources/data_format1/user_info_format1.csv";
    private final static String UserLogFileName = "./src/main/resources/data_format1/user_log_format1.csv";

    public static void runTask(Integer Type, String result)
            throws InterruptedException, IOException, ClassNotFoundException {
        String countRes = result + "/count";
        String sortRes = result + "/sort";
        String topKout = "result";
        if (Count.run(UserLogFileName, UserInfoFileName, countRes, Type)) {
            Sort.run(countRes, sortRes, topKout, 100);
        }
    }

    public static void main(String[] args)
            throws InterruptedException, IOException, ClassNotFoundException {
        String ItemReslt = "./src/main/java/mostpopular/mapreduce/item";
        String SellerReslt = "./src/main/java/mostpopular/mapreduce/seller";
        runTask(1, ItemReslt);
        runTask(2, SellerReslt);
    }
}
