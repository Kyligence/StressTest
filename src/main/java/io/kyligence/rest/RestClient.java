package io.kyligence.rest;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by dongli on 3/6/16.
 */

public class RestClient {

    private int ONE_HOUR_IN_MS = 60 * 60 * 1000;

    private RequestConfig requestConfig = RequestConfig.custom()
            .setSocketTimeout(ONE_HOUR_IN_MS)
            .setConnectTimeout(ONE_HOUR_IN_MS)
            .setConnectionRequestTimeout(ONE_HOUR_IN_MS)
            .build();

    String serverUrl;
    String auth;
    String project;

    public RestClient(String serverUrl, String project, String auth) {
        this.serverUrl = serverUrl;
        this.auth = auth;
        this.project = project;
    }

    /*public boolean disableCache() throws IOException{
        String url = "http://" + serverUrl + "/kylin/api/admin/config";
        CloseableHttpClient client =
                HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();


        HttpPut put = new HttpPut(url);
        put.setHeader("Authorization", "Basic " + auth);
        put.setHeader("Content-Type", "application/json;charset=UTF-8");
        put.setConfig(requestConfig);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("kylin.query.cache-enabled", "false");
        paraMap.put("project", project);
        put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));
        System.out.println(put);
        HttpResponse response = client.execute(put);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        if (response.getStatusLine().getStatusCode() != 200) {
            System.out.println(response);
            return false;
        }else{
            System.out.println("disable cache");
            return true;
        }
    }*/

    public HashMap query(String sql) throws IOException {
        String url = "http://" + serverUrl + "/kylin/api/query";

//        final HttpParams httpParams = new BasicHttpParams();
//        ConnManagerParams.setTimeout(httpParams, 1000);
//        HttpConnectionParams.setConnectionTimeout(httpParams, 10000);
//        HttpConnectionParams.setSoTimeout(httpParams, 10000);
//        HttpClient client = new DefaultHttpClient(httpParams);



        CloseableHttpClient client =
                HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        
        
        HttpPost post = new HttpPost(url);
        post.setHeader("Authorization", "Basic " + auth);
        post.setHeader("Content-Type", "application/json;charset=UTF-8");
        post.setConfig(requestConfig);

        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        post.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));

        HttpResponse response = client.execute(post);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        if (response.getStatusLine().getStatusCode() != 200) {
            return null;
        }

        HashMap resultMap = new ObjectMapper().readValue(result.toString(), HashMap.class);
        /*System.out.println("total scan count : " + (Integer) resultMap.get("totalScanCount"));
        int duration = (Integer) resultMap.get("duration");*/
        return resultMap;
    }
}
