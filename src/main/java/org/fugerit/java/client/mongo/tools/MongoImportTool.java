package org.fugerit.java.client.mongo.tools;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.bson.Document;
import org.fugerit.java.core.cli.ArgUtils;
import org.fugerit.java.core.io.FileIO;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertManyResult;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class MongoImportTool {

	public static final String ARG_MONGO_URL = "mongo-url";
	
	public static final String ARG_FOLDER = "folder";
	
	public static final String ARG_FILTER = "filter";
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void handle( Properties params ) throws Exception {
		String mongoUrl = params.getProperty( ARG_MONGO_URL );
		String folder = params.getProperty( ARG_FOLDER );
		String filter = params.getProperty( ARG_FILTER );
		String connectionString = mongoUrl;
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();
        // Create a new client and connect to the server
        try ( MongoClient mongoClient = MongoClients.create(settings) ) {
        	MongoDatabase db = mongoClient.getDatabase( "what_if" );
        	File file = new File( folder );
        	log.info( "folder {}", file );
        	for ( File current : file.listFiles( ( f ) -> f.getName().endsWith( filter ) ) ) {
        		log.debug( "current file : '{}'", current );
        		int lastDotIndex = current.getName().lastIndexOf( "." );
        		String collectioName = current.getName().substring( 0, lastDotIndex );
        		
        		ObjectMapper mapper = new ObjectMapper();
        		String jsonString = FileIO.readString( current );
        		List<Document> output = (List<Document>) mapper.readValue( jsonString, List.class)
        		            .stream().map(listItem -> new Document((LinkedHashMap)listItem))
        		            .collect(Collectors.toList());
        		if ( output.isEmpty() ) {
        			log.info( "-------- empty collection : {}, size : {}, insert : {}", collectioName, output.size() );
        		} else {
        			MongoCollection<Document> collection = db.getCollection( collectioName );
            		InsertManyResult result = collection.insertMany( output );
            		log.info( "******** insert collection : {}, size : {}, insert : {}", collectioName, output.size(), result.getInsertedIds().size() );
        		}
        		
        	}
        }
	}
	
	public static void main( String [] args ) {
		try {
			 Properties params = ArgUtils.getArgs( args );
			 MongoImportTool tool = new MongoImportTool();
			 tool.handle(params);
		} catch (Exception e) {
			log.error( "Error : "+e, e );
		}
	}
	
}
