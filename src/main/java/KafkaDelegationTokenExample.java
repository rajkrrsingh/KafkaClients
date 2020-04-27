import com.google.common.io.Resources;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.token.delegation.DelegationToken;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/*
 * how to run:
 * java -Djava.security.krb5.conf="/tmp/krb.conf"  -cp <>.jar KafkaDelegationTokenExample "hostname:9092" "/tmp/my.client.keytab" "kafka/_HOST@REALM" kafkatopic
 */
public class KafkaDelegationTokenExample {
    public static void main(String[] args) throws Exception {
        if(args.length<1){
            System.out.println("usage : java -cp <>.jar Broker-list keytab-loc client-principal topic ");
            System.exit(-1);
        }
        // obtain the delegation token and set it to the ScramLoginModule jaas config
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required tokenauth=true username=\"%s\" password=\"%s\";";
        DelegationToken dt = getToken(args);
        String jaasCfg = String.format(jaasTemplate, dt.tokenInfo().tokenId(), dt.hmacAsBase64String());
        System.out.println("Kafka Client Jaas succefully generated at :"+jaasCfg);

        //Run Kafka clients in seperate threads
        Thread consumerThread = new Thread(new ConsumerThread(args[3],jaasCfg),"consumer-thread");
        Thread producerThread = new Thread(new ProducerThread(args[3],jaasCfg),"producer-thread");
        consumerThread.start();
        producerThread.start();
        consumerThread.join();
        producerThread.join();


    }

    private static DelegationToken getToken(String[] args) throws InterruptedException, java.util.concurrent.ExecutionException {
        String jaasTemplate = "com.sun.security.auth.module.Krb5LoginModule required debug=true useKeyTab=true storeKey=true serviceName=\"kafka\" keyTab=\"%s\" principal=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, args[1], args[2]);
        System.out.println("Creating AdminClient with jass config : "+jaasCfg);

        Properties adminClientProp = new Properties();
        adminClientProp.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,args[0]);
        adminClientProp.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        adminClientProp.put(SaslConfigs.SASL_JAAS_CONFIG,jaasCfg);

        AdminClient adminClient = AdminClient.create(adminClientProp);

        System.out.println("Creating token");
        CreateDelegationTokenOptions createDelegationTokenOptions = new CreateDelegationTokenOptions();
        CreateDelegationTokenResult createDelegationTokenResult = adminClient.createDelegationToken(createDelegationTokenOptions);
        DelegationToken dt = createDelegationTokenResult.delegationToken().get();
        System.out.println("Got dt : "+dt);
        return dt;
    }

}

class ConsumerThread implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private String topicName;
    private String jaasConfig;

    public ConsumerThread(String topicName,String jassConfig){
        this.topicName = topicName;
        this.jaasConfig = jassConfig;
        init();
    }

    private void init()  {
        try
        {
            InputStream props = Resources.getResource("consumer.props").openStream();
            Properties properties = new Properties();
            try {
                properties.load(props);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,this.jaasConfig);
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topicName));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    System.out.println("\n");
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}

class ProducerThread implements Runnable {
    private String topicName;
    private String jaasConf;
    private KafkaProducer<String, String> producer;

    public ProducerThread(String topicName,String jaasConf) {
        this.jaasConf = jaasConf;
        this.topicName = topicName;
        init();
    }

    private void init() {
        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,this.jaasConf);
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        try {
            int i=0;
            while (true) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,"key-"+i,"value-"+i);
                System.out.println(record);
                producer.send(record);
                i++;
                Thread.sleep(1000);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }
    }
}
