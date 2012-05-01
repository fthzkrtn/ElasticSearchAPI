package tr.gov.eba.elastictest;

import org.apache.http.client.HttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.lang.model.element.Element;
import javax.xml.ws.http.HTTPException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.*;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.*;
import org.elasticsearch.node.NodeBuilder;

/**
 *
 * @author fatih
 */
public class ElasticSearchAPI {

    public String[] params = {"GET", "POST", "BULK"};
    public ArrayList<Element> paramList = new ArrayList(Arrays.asList(params));
    private static Statement stmt;
    private static Statement stSize;
    private static ResultSet rs;
    private static ResultSet rsSize;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String postData = GenerateSource().toString();
        _useElasticAPI("BULK", postData);
    }   //  End of main

    // TODO write a classs to generate source
    public static Gson GenerateSource(){
        Gson response = new Gson();
        try {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(ElasticSearchAPI.class.getName()).log(Level.SEVERE, null, ex);
            }
                String rstSize = "";
                String source;
                Integer iId = 1;
                String url = "jdbc:mysql://10.30.4.28:3306/eba";
                Connection con = DriverManager.getConnection(url, "eba", "eba");
                stmt = (Statement) con.createStatement();
                stSize = (Statement) con.createStatement();
                rs = stmt.executeQuery("SELECT  eba.video_store.video_id, "+ 
                        "eba.video_store.caption, "  +
                        "eba.video_store.video_text " +
                        "FROM eba.video_store " +
                        "ORDER BY eba.video_store.video_id ASC LIMIT 5");
                rsSize = stSize.executeQuery("SELECT count(*) as rsSize FROM eba.video_store");
                while(rsSize.next()){
                    rstSize = rsSize.getString("rsSize");
                }
                //source = "{ \"videos\": [ ";
                source = "";
                while(rs.next()){
                    source += "{";
                    source += "\"video_id\": \"" + rs.getString("video_id") + "\",";
                    source += "\"video_caption\": \"" + rs.getString("caption").replaceAll("\"","'") + "\",";
                    source += "\"video_text\": \"" + rs.getString("video_text").replaceAll("\"","'") + "\"";
                    //if ( Integer.parseInt(rstSize)  != rs.getRow() ){
                       // source += "},";
                    //}else{
                        source += "}";
                        //indexResults(source, "videos", "video");
                    //}
                }
                //source += " ] }";
                System.out.print(source);
                System.out.print(response.toJson(source));
            }  catch (SQLException e) {
                System.out.print("Error occured on sql: " + e);
            }
        return response;
    }
    
    private static void _useElasticAPI(String param, String source){
        HttpClient client = new DefaultHttpClient();
        client.getParams().setParameter("http.useragent", "Test Client");
        HttpPost post = new HttpPost("http://localhost:9200");
        HttpGet get = new HttpGet("http://localhost:9200");
        Node node = NodeBuilder.nodeBuilder().node();
        Client _nodeClient = node.client();


        try {
            HttpResponse response = null;
            /*
             * used indexof to set coming parameter to be used in switch as an integer becasue JRE version 1.6 does not provide
             * string support in switch case
             * CHANGELOG JRE 1.7 recently provide string support to use strings in switch statements 
             */
            switch (param.indexOf(param)){
                case 1: //  GET index 
                    response = client.execute(get);
                    break;
                case 2: // POST index
                    response = client.execute(post);
                    break;
                case 3: // BULK index
                    BulkRequestBuilder _bulkBuilder = _nodeClient.prepareBulk();
                    _bulkBuilder.add(_nodeClient.prepareIndex("videos", "video", "1")
                            .setSource(""));
                    break;
            }
            if ( response != null ){
                System.out.println(response.getStatusLine());
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream stream = entity.getContent();
                    try {
                        BufferedReader bf = new BufferedReader(
                                new InputStreamReader(stream));
                        System.out.println(bf.readLine());
                    } catch (IOException e) {
                    } catch (RuntimeException e) {
                        post.abort();
                        throw e;
                    } finally {
                        stream.close();
                    }
                    client.getConnectionManager().shutdown();
                }   //  End of  inner if 
            }   // End of outer if
            
        } catch (HTTPException e) {
            throw e;
        } catch (IOException e) {
        }   //  end of IOException 
    }   // end of _useElasticAPI
   
}
