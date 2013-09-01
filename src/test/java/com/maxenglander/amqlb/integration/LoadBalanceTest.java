package com.maxenglander.amqlb.integration;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxenglander
 * 
 * Test that the transport load balances requests among multiple brokers.
 */
public class LoadBalanceTest {
    private static final String BROKER_URI_FORMAT = "tcp://localhost:6161%d";
    private static final String MESSAGE_FORMAT = "message%d";
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalanceTest.class);
    private static final int NUM_BROKERS = 2;
    private static final int NUM_MESSAGES = 10;
    private static final String QUEUE_NAME = "test_queue";
    
    private static BrokerService[] BROKER_SERVICES;    
    
    private Connection connection;
    private Session session;
    private MessageProducer producer;    
    
    @BeforeClass
    public static void setupOnce() throws Exception {
        BROKER_SERVICES = new BrokerService[NUM_BROKERS];
        for(int i = 0; i < NUM_BROKERS; i++) {
            BROKER_SERVICES[i] = new BrokerService();
            BROKER_SERVICES[i].addConnector(String.format(BROKER_URI_FORMAT, i));            
            BROKER_SERVICES[i].setBrokerName("broker" + i);
            BROKER_SERVICES[i].setDeleteAllMessagesOnStartup(true);
            BROKER_SERVICES[i].start();
        }
    }    
    
    @Before
    public void setup() throws JMSException {
        StringBuilder uriBuilder = new StringBuilder();
        uriBuilder.append("lb:(");
        for(int i = 0; i < NUM_BROKERS; i++) {
            uriBuilder.append(String.format(BROKER_URI_FORMAT, i));
            uriBuilder.append(",");            
        }
        uriBuilder.deleteCharAt(uriBuilder.length() - 1).append(")");

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uriBuilder.toString());
        connection = connectionFactory.createConnection();
        
        LOGGER.debug("Creating sessions");       
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        LOGGER.debug("Created session");

        producer = session.createProducer(session.createQueue(QUEUE_NAME));
        
        LOGGER.debug("Starting connection");
        connection.start();
        LOGGER.debug("Started connection");
    }

    @After
    public void tearDown() throws JMSException {
        producer.close();
        session.close();
        connection.stop();
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
        for(int i = 0; i < NUM_BROKERS; i++) {
            BROKER_SERVICES[i].stop();
            BROKER_SERVICES[i].deleteAllMessages();
        }
    }
    
    @Test
    public void testSendsAreDistributedToBrokers() throws JMSException, Exception {
        // Send NUM_MESSAGES to each broker
        for(int i = 0; i < NUM_MESSAGES * NUM_BROKERS; i++) {
            System.out.println("Sending message " + i);
            Message message = session.createTextMessage(String.format(MESSAGE_FORMAT, i));
            producer.send(message);            
        }

        ActiveMQDestination activeMQDestination = 
                ActiveMQDestination.createDestination(QUEUE_NAME, ActiveMQDestination.QUEUE_TYPE);                    
        for(int i = 0; i < NUM_BROKERS; i++) {
            BrokerService brokerService = BROKER_SERVICES[i];            
            Broker broker = brokerService.getBroker();
            Destination testQueueDestination = broker.getDestinationMap().get(activeMQDestination);
            int messageCount = testQueueDestination.getMessageStore().getMessageCount();
            assertEquals(NUM_MESSAGES, messageCount);                    
        }
    }
}