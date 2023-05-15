package test.org.fugerit.java.client.mongo;

import static org.junit.Assert.fail;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestMongoConn {

	@Test
	public void testMongo() {
		log.info( "test start" );
		
		 String connectionString = "mongodb+srv://test_user:MongoClient_2023@fj-test.x5reacv.mongodb.net/?retryWrites=true&w=majority";
	        ServerApi serverApi = ServerApi.builder()
	                .version(ServerApiVersion.V1)
	                .build();
	        MongoClientSettings settings = MongoClientSettings.builder()
	                .applyConnectionString(new ConnectionString(connectionString))
	                .serverApi(serverApi)
	                .build();
	        // Create a new client and connect to the server
	        try (MongoClient mongoClient = MongoClients.create(settings)) {
	            try {
	                // Send a ping to confirm a successful connection
	                MongoDatabase database = mongoClient.getDatabase("admin");
	                database.runCommand(new Document("ping", 1));
	                log.info( "*****************************************************************" );
	                log.info( "* Pinged your deployment. You successfully connected to MongoDB *" );
	                log.info( "*****************************************************************" );
	            } catch (MongoException e) {
	               String message = "Connection error : "+e;
	               log.error(message, e );
	               fail( message );
	            }
	        }
		
		log.info( "test end" );
	}
	
}
