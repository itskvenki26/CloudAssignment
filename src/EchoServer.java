import java.io.File;
import java.io.IOException;


import java.io.PrintWriter;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import java.util.*;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


/** 
 * @ServerEndpoint gives the relative name for the end point
 * This will be accessed via ws://localhost:8080/EchoChamber/echo
 * Where "localhost" is the address of the host,
 * "EchoChamber" is the name of the package
 * and "echo" is the address to access this class from the server
 */
@ServerEndpoint("/echo") 
public class EchoServer {
	/**
	 * @OnOpen allows us to intercept the creation of a new session.
	 * The session class allows us to send data to the user.
	 * In the method onOpen, we'll let the user know that the handshake was 
	 * successful.
	 */
	@OnOpen
	public void onOpen(Session session){
		System.out.println(session.getId() + " has opened a connection"); 
		try {
			session.getBasicRemote().sendText("Connection Established");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * When a user sends a message to the server, this method will intercept the message
	 * and allow us to react to it. For now the message is read as a String.
	 */
	@OnMessage
	public void onMessage(String message, Session session){
		System.out.println("Message from " + session.getId() + ": " + message);
		AWSCredentials credentials = null;
		try {
			try {
	            credentials = new ProfileCredentialsProvider().getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (~/.aws/credentials), and is in valid format.",
	                    e);
	        }
			
			AmazonSQS sqs = new AmazonSQSClient(credentials);
	        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	        sqs.setRegion(usWest2);

//			This is how we can list all sqs we created		        
//	        for (String queueUrl : sqs.listQueues().getQueueUrls()) {
//                System.out.println("  QueueUrl: " + queueUrl);
//            }
	        
	        ListQueuesResult allsqs = sqs.listQueues();
	        
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest("tweetsfinal" + message);
	        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
	        
			ConfigurationBuilder cb = new ConfigurationBuilder();
			System.out.println("inside twitter");
			cb.setOAuthConsumerKey("3iSEndEYy9qjaqui8r8A3DlWD");
			cb.setOAuthConsumerSecret("LZ7O6arrSEXKCSXP6CiS4D12oAb4FuA9Q24hxcBVJ9b8zS55na");
			cb.setOAuthAccessToken("363159363-BEfNnE68kXbYxTcUdOa4APoRlQTUpuAD1mzSXbZQ");
			cb.setOAuthAccessTokenSecret("bGaopNSLdf3TEMFDN6CrfLU3u06pSeyFkAWRnoEOidBcv");

			Twitter twitter = new TwitterFactory(cb.build()).getInstance();
			Query query = new Query(message);
			int numberOfTweets = 5120;
			long lastID = Long.MAX_VALUE;
			ArrayList<Status> tweets = new ArrayList<Status>();

			while (tweets.size () < numberOfTweets) {
				if (numberOfTweets - tweets.size() > 100)
					query.setCount(100);
				else 
					query.setCount(numberOfTweets - tweets.size());
				try 
				{ 
					QueryResult result = twitter.search(query);
					tweets.addAll(result.getTweets());
					System.out.println("Gathered " + tweets.size() + " tweets");

					//storing the tweets in the current page to newhappy.txt i.e NOSQL
					for (Status t: tweets) {
						//System.out.println(t.getText());
						if(t.getId() < lastID) lastID = t.getId();
						if(t.getGeoLocation() != null)
						{

						System.out.println(t.getGeoLocation().getLatitude() + "|" + t.getGeoLocation().getLongitude());
						sqs.sendMessage(new SendMessageRequest(myQueueUrl, t.getText()));
						session.getBasicRemote().sendText(t.getGeoLocation().getLatitude() + "|" + t.getGeoLocation().getLongitude());
						}

					}

					session.getBasicRemote().sendText(message);
				} catch (IOException | TwitterException ex) {
					ex.printStackTrace();

				}
			}
			
		}
		finally{		}
	}

				/**
				 * The user closes the connection.
				 * 
				 * Note: you can't send messages to the client from this method
				 */
				@OnClose
				public void onClose(Session session){
					System.out.println("Session " +session.getId()+" has ended");
				}
			}