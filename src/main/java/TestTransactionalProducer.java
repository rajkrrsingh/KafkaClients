import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestTransactionalProducer {
    private static final Logger log = LoggerFactory.getLogger(TestTransactionalProducer.class);
    public static void main(String[] args) throws IOException {
        if(args.length<1){
            System.out.println("usage : java -cp <>.jar TestTransactionalProducer <topic>");
            System.exit(-1);
        }

        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer");
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"test-txn-id");
            properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
            properties.put(ProducerConfig.ACKS_CONFIG,"all");
            producer = new KafkaProducer<>(properties);
        }
        producer.initTransactions();
        try {

            int i=0;
            while(i<=100) {
                //ProducerRecord<String, String> record = new ProducerRecord<>(args[0], "value-" + i);
                producer.beginTransaction();
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0],"key-"+i,"value-"+i);
                //Thread.sleep(10000);
                producer.send(record);
                i++;
                log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | send "+record);
                producer.commitTransaction();
            }
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        } finally {
            producer.close();
        }

    }
    }


