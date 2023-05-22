package test.org.fugerit.java.client.mongo;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MongoTestSingleton {

	//public static String MONGO_CONNECTION_STRING = "mongodb+srv://test_user:MongoTest_2023@cluster0.utsqxc0.mongodb.net/?retryWrites=true&w=majority&authSource=fj_client_mongo";
	
	public static String MONGO_CONNECTION_STRING = "mongodb+srv://admin:TuttiFrutti_2015@fj-test.soalsea.mongodb.net/?retryWrites=true&w=majority";
	
	
	public static String MONGO_DATABASE_NAME = "fj_client_mongo";
	
	@Getter private MongoClient client;
	
	@Getter private static MongoTestSingleton instance = new MongoTestSingleton();
	
	private MongoTestSingleton() {
		 String connectionString = MONGO_CONNECTION_STRING;
	        ServerApi serverApi = ServerApi.builder()
	                .version(ServerApiVersion.V1)
	                .build();
	        MongoClientSettings settings = MongoClientSettings.builder()
	                .applyConnectionString(new ConnectionString(connectionString))
	                .serverApi(serverApi)
	                .build();
	        // Create a new client and connect to the server
	        MongoTestSingleton singleton = new MongoTestSingleton();
	        MongoClient mongoClient = MongoClients.create(settings);
            // Send a ping to confirm a successful connection
            ping( mongoClient );
            singleton.client = mongoClient;
	}

	private static void ping( MongoClient mongoClient ) {
		 MongoDatabase database = mongoClient.getDatabase( MONGO_DATABASE_NAME );
         database.runCommand(new Document("ping", 1));
         log.info( "*****************************************************************" );
         log.info( "* Pinged your deployment. You successfully connected to MongoDB *" );
         log.info( "*****************************************************************" );
	}
	
	public static void end() {
		if ( getInstance() != null && getInstance().getClient() != null ) {
			getInstance().getClient().close();
			log.info( "client closed!" );
		}
	}

	public void ping() {
		ping( this.getClient() );
	}
	
	@Override
	protected void finalize() throws Throwable {
		end();
	}
	
}
