package org.cjt.aggregate;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MQTest {

    ConnectionFactory connectionFactory;
    Connection connection;
    Session session;
    TemporaryQueue tempQueue;

    /* I'm ignoring all these test cases for now, i really can't 
     * fully test an entire system on my laptop, so lets assume that 
     * our Queue consumer is as fast as it can be 
     */

    @Ignore
    @Before
    public void setupBroker() throws JMSException {
        connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        tempQueue = session.createTemporaryQueue();
    }

    @Ignore
    @Test
    public void populateQueue() throws JMSException {
        MessageProducer producer = session.createProducer(tempQueue);
        for(int i = 0; i < 1000; i++) {
            TextMessage msg = session.createTextMessage("Foo");
            producer.send(msg);
        }
    }

    @Ignore
    @After
    public void destroyBroker() {
        connectionFactory = null;
    }
}
