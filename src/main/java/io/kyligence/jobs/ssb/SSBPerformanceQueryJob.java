package io.kyligence.jobs.ssb;

import io.kyligence.rest.RestClient;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by dongli on 3/6/16.
 */
public class SSBPerformanceQueryJob {
    int repeat_times = 3;

    RestClient restClient;
    File queries;

    private int totalScanCount = -1;

    public SSBPerformanceQueryJob(String serverUrl, String project, String auth, String query) {
        restClient = new RestClient(serverUrl, project, auth);
        queries = new File(query);
    }

    public void run() throws IOException {
        final String QUERY_LINE_SEP = "============================================";
        /*boolean isDisableCache = restClient.disableCache();
        if(!isDisableCache){
            System.out.println("can not disable cache.");
            return;
        }*/
        if (!queries.exists()) {
            System.out.println("No query found.");
            return;
        } else if (queries.isDirectory()) {
            File[] files = queries.listFiles();
            Arrays.sort(files, new Comparator<File>() {
                public int compare(File o1, File o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            for (File queryFile : files) {
                String sql = FileUtils.readFileToString(queryFile);
                long duration = getSqlDuration(sql);
                System.out.println(QUERY_LINE_SEP);
                System.out.println("Query file: " + queryFile.getName());
                //System.out.println("SQL: " + sql);
                System.out.println("totalScanCount : " + totalScanCount);
                System.out.println("duration: " + duration + " ms");
                System.out.println(QUERY_LINE_SEP);
            }
        } else if (queries.isFile()) {
            List<String> lines = FileUtils.readLines(queries);
            for (String sql : lines) {
                long duration = getSqlDuration(sql);
                System.out.println(QUERY_LINE_SEP);
                System.out.println("Query file: " + queries.getName());
                //System.out.println("SQL: " + sql);
                System.out.println("totalScanCount : " + totalScanCount);
                System.out.println("duration: " + duration);
                System.out.println(QUERY_LINE_SEP);
            }
        }
    }

    private long getSqlDuration(String sql) {
        long duration = 0;
        try {
            long[] durations = new long[repeat_times];
            for (int i = 0; i < repeat_times; i++) {
                Map result = restClient.query(sql);
                durations[i] = (Integer)result.get("duration");
                totalScanCount = (Integer)result.get("totalScanCount");
            }
            if (repeat_times == 1) {
                return durations[0];
            }

            Arrays.sort(durations);
            long sum = 0;
            for (int i = 0; i < repeat_times; i++) {
                sum += durations[i];
            }
            duration = sum / (repeat_times);
            return duration;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static void main(String args[]) throws Exception {
        if(args == null || args.length < 4){
            throw new Exception("Wrong argument format. Usage : serverUrl project auth query");
        }
        String serverUrl = args[0];//"sandbox:7070";
        String project = args[1];//"default";
        String auth = args[2] ;  //"QURNSU46S1lMSU4=";
        String query = args[3] ; //"query";
        new SSBPerformanceQueryJob(serverUrl, project, auth, query).run();
    }
}
