
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnection;
import javax.jms.*;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) throws Exception {

        // thread(new HelloWorldProducer(), false);

        thread(new HelloWorldConsumer("topology.>"), false);
        thread(new HelloWorldConsumer("instance.status.>"), false);


    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {

        String topicName;

        public HelloWorldProducer(String topic) {
            this.topicName = topic;
        }

        public void run() {
            try {

                // Create a ConnectionFactory
                String url = ActiveMQConnection.DEFAULT_BROKER_URL;
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                // Destination destination = session.createQueue("TEST.FOO");

                Topic topic = session.createTopic(topicName);

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(topic);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                TextMessage message = session.createTextMessage(text);

                // Tell the producer to send the message
                System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
                producer.send(message);

                // Clean up
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {

        String topicName;

        public HelloWorldConsumer(String topic) {
            this.topicName = topic;
        }

        public void run() {
            try {

                boolean close = false;

                String url = ActiveMQConnection.DEFAULT_BROKER_URL;
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();
                connection.setExceptionListener(this);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                Topic topic = session.createTopic(topicName);
                MessageConsumer consumer = session.createConsumer(topic);

                consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        processMessage(message);
                    }
                });

                //change these to close connections
                if (close) {
                    consumer.close();
                    session.close();
                    connection.close();
                }

            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }

        private void processMessage(Message message) {

            System.out.println("RRR - " + message);

            if (message instanceof TextMessage) {
                String text = null;
                try {
                    TextMessage textMessage = (TextMessage) message;
                    text = textMessage.getText();
                    System.out.println("TextMessage - " + ((TextMessage) message).getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }

            if (message instanceof ActiveMQBytesMessage) {
                System.out.println("Byte Message - " + message);

            }

            if (message instanceof ActiveMQTextMessage) {
                try {
                    String text = ((ActiveMQTextMessage) message).getText();
                    System.out.println("ACT TM - " + text);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Received: " + message);
            }
        }
    }
}