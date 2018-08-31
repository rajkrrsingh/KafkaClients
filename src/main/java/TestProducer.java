import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
 
public class TestProducer {
    final static Logger logger = Logger.getLogger(TestProducer.class);
    public static void main(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();


 
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.200.5.214:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("producer.type", "sync");
        props.put("topic.metadata.refresh.interval.ms","600000");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
               producer.send(data);
               logger.info("producer send data "+msg);
               try{
                   logger.info("going to sleep");
                Thread.sleep(10000);
                logger.info("waking up after 100000 ms");
               }catch(Exception e){
                e.printStackTrace();
               }
        }
        producer.close();
    }
}