import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class SensorConsumer{

private static final Logger log = LoggerFactory.getLogger(SensorConsumer.class);
    public static void main(String[] args) throws Exception{

            String topicName = "kafkatopic1";
            KafkaConsumer<String, String> consumer = null;
            int rCount;

            Properties props = new Properties();
            props.put("bootstrap.servers", "hdp26:6667");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("enable.auto.commit", "false");
           // props.put("auto.offset.reset","none");

            consumer = new KafkaConsumer<>(props);
            TopicPartition p0 = new TopicPartition(topicName, 0);
            //TopicPartition p1 = new TopicPartition(topicName, 1);
            //TopicPartition p2 = new TopicPartition(topicName, 2);

            //consumer.assign(Arrays.asList(p0,p1,p2));
            consumer.assign(Arrays.asList(p0));
        log.info("HDP: Called Assign");

        //System.out.println("Current position p0=" + consumer.position(p0)
             //                + " p1=" + consumer.position(p1)
        //+ " p2=" + consumer.position(p2));
           //System.out.println("Current position p0=" + consumer.position(p0));
           log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | \"Current position p0=\" "+consumer.position(p0));
           log.info("consumer postition ends");

            consumer.seek(p0, getOffsetFromDB(p0));
        log.info("HDP: Called Seek");
            //consumer.seek(p1, getOffsetFromDB(p1));
            //consumer.seek(p2, getOffsetFromDB(p2));
            //System.out.println("New positions po=" + consumer.position(p0)
              //               + " p1=" + consumer.position(p1)
                //             + " p2=" + consumer.position(p2));
            //System.out.println("New positions po=" + consumer.position(p0));
            log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | \"New positions po=\" "+consumer.position(p0));
            //System.out.println("Start Fetching Now");
            log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | \"Start Fetching Now\" ");
            try{
                log.info("HDP: Calling poll");
                do{
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    //System.out.println("Record polled " + records.count());
                    log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | \"Record polled \" "+records.count());
                    rCount = records.count();
                    for (ConsumerRecord<String, String> record : records){
                        saveAndCommit(consumer,record);
                    }
                }while (true);
            }catch(Exception ex){
                //System.out.println("Exception in main.");
                log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | Exception in main.");
                ex.printStackTrace();
            }
            finally{
                    consumer.close();
            }
    }

    private static long getOffsetFromDB(TopicPartition p){
        long offset = 0;
        try{
                Class.forName("com.mysql.jdbc.Driver");
                Connection con= DriverManager.getConnection("jdbc:mysql://hdp26:3306/hive","hive","hive");

                String sql = "select offset from tss_offsets where topic_name='" + p.topic() + "' and part=" + p.partition();
                Statement stmt=con.createStatement();
                ResultSet rs = stmt.executeQuery(sql);
                if (rs.next())
                    offset = rs.getInt("offset");
                stmt.close();
                con.close();
            }catch(Exception e){
                System.out.println("Exception in getOffsetFromDB");
                log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | Exception in getOffsetFromDB");
                e.printStackTrace();
            }
        return offset;
    }

    private static void saveAndCommit(KafkaConsumer<String, String> c, ConsumerRecord<String, String> r){
        String msg= "Topic=" + r.topic() + " Partition=" + r.partition() + " Offset=" + r.offset() + " Key=" + r.key() + " Value=" + r.value();
        log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | "+msg);
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection("jdbc:mysql://hdp26:3306/hive","hive","hive");
            con.setAutoCommit(false);

            String insertSQL = "insert into tss_data values(?,?)";
            PreparedStatement psInsert = con.prepareStatement(insertSQL);
            psInsert.setString(1,r.key());
            psInsert.setString(2,r.value());

            String updateSQL = "update tss_offsets set offset=? where topic_name=? and part=?";
            PreparedStatement psUpdate = con.prepareStatement(updateSQL);
            psUpdate.setLong(1,r.offset()+1);
            psUpdate.setString(2,r.topic());
            psUpdate.setInt(3,r.partition());

            psInsert.executeUpdate();
            psUpdate.executeUpdate();
            con.commit();
            con.close();
        }catch(Exception e){
           // System.out.println("Exception in saveAndCommit");
            log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | Exception in saveAndCommit");
            e.printStackTrace();
        }

    }
}