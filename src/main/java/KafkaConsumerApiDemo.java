import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;

public class KafkaConsumerApiDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApiDemo.class);


    public static void main(String[] args) throws Exception {
        {
            if(args.length<1){
                System.out.println("usage : java -cp <>.jar <topic1> <topic2> ..");
                System.exit(-1);
            }

            List<String> topics = new ArrayList<>();
            for(String arg:args){
                topics.add(arg);
            }

            KafkaConsumer<String, String> consumer;
            try (InputStream props = Resources.getResource("consumer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                if (properties.getProperty("group.id") == null) {
                    properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
                }
                consumer = new KafkaConsumer<String, String>(properties);
            }
            //consumeMessages(consumer,topics);
            seekAndConsumeMessage(consumer,"kafkatopic");



        }
    }

    public static void consumeMessages(KafkaConsumer consumer,List<String> topics) throws Exception{
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offset = %d, key = %s, value = %s "
                        +record.offset()+","+record.key()+","+record.value());
            }
        }
    }

    public static void seekAndConsumeMessage(KafkaConsumer consumer,String topicName) throws Exception{
        TopicPartition topicPartition = new TopicPartition(topicName,0);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition,568);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records){
                logger.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | offset = %d, key = %s, value = %s "
                        +record.offset()+","+record.key()+","+record.value());
            }
        }
    }

}
