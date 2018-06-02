import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by lb on 2018/6/2.
 */
public class Producer {
    public static final String KAFKA_ADDRESS = "192.168.204.128:9092";
    public static final String TOPIC = "mykafka";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG ,org.apache.kafka.clients.producer.internals.DefaultPartitioner.class) ;

        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(props);
        String topic = TOPIC;
        for(int i=0;i<10;i++){
            String key = "key"+i;
            String value = "value"+i;
            kp.send(new ProducerRecord<String, String>(topic, key, value),new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null != exception) {
                        System.out.println(exception.getMessage() + exception);
                    }else{
                        System.out.println("记录的offset在:" + metadata.offset());
                    }
                }
            });
        }
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {

        }
        kp.close();

    }



}
