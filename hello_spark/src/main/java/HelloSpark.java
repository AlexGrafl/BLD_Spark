import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Alex on 04.06.2016.
 */


public class HelloSpark {
    private static final int NUM_SAMPLES = 10000000;

    public static void main(String[] args){

        System.out.println("Working Directory = " +
                System.getProperty("user.dir"));
        String appName = "PI ESTIMATION";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("spark://192.168.0.17:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("target/spark1-1.0-SNAPSHOT.jar");
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return x * x + y * y < 1;
            }
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}
