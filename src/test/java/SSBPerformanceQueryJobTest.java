import io.kyligence.jobs.ssb.SSBPerformanceQueryJob;
import org.junit.Test;

/**
 * Created by xiefan on 16-12-27.
 */
public class SSBPerformanceQueryJobTest {
    @Test
    public void testInKylin() throws Exception{
        String serverUrl = "master:7170";
        String project = "ssb"; //"default";
        String auth = "QURNSU46S1lMSU4="; //"QURNSU46S1lMSU4=";
        String queryDir = "src/test/resources/query_scale10/"; //"query";
        new SSBPerformanceQueryJob(serverUrl, project, auth, queryDir).run();
    }

    @Test
    public void testInKAP() throws Exception{
        String serverUrl = "master:7070";
        String project = "ssb"; //"default";
        String auth = "QURNSU46S1lMSU4="; //"QURNSU46S1lMSU4=";
        String queryDir = "src/test/resources/query_scale10/"; //"query";
        new SSBPerformanceQueryJob(serverUrl, project, auth, queryDir).run();
    }
}
