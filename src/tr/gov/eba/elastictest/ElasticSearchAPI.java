package tr.gov.eba.elastictest;

import com.google.gson.Gson;
import java.io.IOException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.ws.http.HTTPException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.node.*;
import org.elasticsearch.node.NodeBuilder;

/**
 *
 * @author fatih
 */
public class ElasticSearchAPI {

    public static String[] params = {"GET", "POST", "BULK"};
    public static ArrayList<String> paramList = new ArrayList(Arrays.asList(params));
    public static Gson gson = new Gson();
    private static Statement stmt;
    private static Statement stSize;
    private static ResultSet rs;
    private static ResultSet rsSize;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            _useElasticAPI("BULK", GenerateSource());
            //GenerateSource();
        } catch (IOException ex) {
           Logger.getLogger(ElasticSearchAPI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }   //  End of main

    public static class Video{
        private String VideoId;
        private String VideoCaption;
        private String VideoText;
    }
    private static void _useElasticAPI(String param, ArrayList<Video> videos) throws IOException {
        Node node = NodeBuilder.nodeBuilder().client(true).node();
        XContentBuilder xb = new XContentFactory().jsonBuilder();
        Client _nodeClient = node.client();
        try {
            /*
             * used indexof to set coming parameter to be used in switch as an
             * integer becasue JRE version 1.6 does not provide string support
             * in switch case CHANGELOG JRE 1.7 recently provide string support
             * to use strings in switch statements
             */
            switch (paramList.indexOf(param)) {
                case 0: //  GET index 
                    //response = client.execute(get);
                    break;
                case 1: // POST index
                    //response = client.execute(post);
                    break;
                case 2: // BULK index
                    BulkRequestBuilder _bulkBuilder = _nodeClient.prepareBulk();
                    Integer seq = 1;
                    if ( !videos.isEmpty() ){
                        for ( Video video: videos ){
                            System.out.print(video.VideoId + "\n");
                            System.out.print(video.VideoCaption + "\n");
                            System.out.print(video.VideoText + "\n");
                            IndexResponse response = _nodeClient.prepareIndex("videos", "video", seq.toString())
                                .setSource("{" +
                                                "\"video_id\": \"" + video.VideoId.toString() +"\","+
                                                "\"video_caption\": \"" + video.VideoCaption.toString() +"\"," +
                                                "\"video_text\": \"" + video.VideoText.toString() +"\"" +
                                            "}")
                                .execute()
                                .actionGet();
                            System.out.print(response.toString());
                            /*_bulkBuilder.add(_nodeClient.prepareIndex("videos", "video", seq.toString())
                                .setSource("{" +
                                                "\"video_id\": \"" + video.VideoId.toString() +"\","+
                                                "\"video_caption\": \"" + video.VideoCaption.toString() +"\"," +
                                                "\"video_text\": \"" + video.VideoText.toString() +"\"" +
                                            "}")
                                );*/
                            seq++;
                        }
                        /*BulkResponse _bulkResponse = _bulkBuilder.execute().actionGet();
                        if ( _bulkResponse.hasFailures() ){
                            System.out.print("Error occured on bulkresponse");
                        }*/
                        node.close();
                    }
                    break;
            }
        } catch (HTTPException e) {
            throw e;
        }   //  end of IOException 
    }   // end of _useElasticAPI
    
    // TODO write a classs to generate source
    public static ArrayList<Video> GenerateSource() {
        String response = "";
        ArrayList<Video> videos = new ArrayList<Video>();
        try {
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(ElasticSearchAPI.class.getName()).log(Level.SEVERE, null, ex);
            }
            String rstSize = "";
            String source;
            Integer iId = 1;
            String url = "jdbc:mysql://10.30.4.28:3306/eba?useUnicode=true&characterEncoding=UTF-8";
            Connection con = DriverManager.getConnection(url, "eba", "eba");
            stmt = (Statement) con.createStatement();
            stSize = (Statement) con.createStatement();
            //rs = stmt.executeQuery("SHOW VARIABLES LIKE 'character\\_set\\_%'");
            rs = stmt.executeQuery("SELECT  eba.video_store.video_id, "
                    + "eba.video_store.caption, "
                    + "eba.video_store.video_text "
                    + "FROM eba.video_store "
                    + "ORDER BY eba.video_store.video_id ASC LIMIT 500");
            rsSize = stSize.executeQuery("SELECT count(*) as rsSize FROM eba.video_store");
            while (rsSize.next()) {
                rstSize = rsSize.getString("rsSize");
            }
            while (rs.next()) {
                Video myVideo = new Video();
                myVideo.VideoId = rs.getString("video_id");
                myVideo.VideoCaption = rs.getString("caption").replaceAll("\"", "'") ;
                myVideo.VideoText = rs.getString("video_text").replaceAll("\"", "'");
                videos.add(myVideo);
            }
            if ( videos.isEmpty() ){
                response = gson.toJson(videos);
                System.out.print(videos);
                System.out.print(gson.toJson(videos));
            }
        } catch (SQLException e) {
            System.out.print("Error occured on sql: " + e);
        }
         System.out.print(gson.toJson(videos));
        return videos;
    }   //  End of GenerateSource
}
