import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import java.io.InputStream;
import java.util.*;

public class MyConsumer {
    final static Logger logger = Logger.getLogger(MyConsumer.class);

    public static void main(String[] args) throws Exception {
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


        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println("\n");
            }

        }
    }
}