package io.com.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys
{
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        //System.out.println("Hello World");
        log.info("I am a Kafka Producer");


        //Create Producer Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //Create a producer record

        for(int i=0; i<10; i++)
        {
            String topic = "demo_java";
            String key = "id_"+i;
            String value = "Hello World "+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //Send Data -async
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes everytime a record is successfully sent or a exception is thrown

                    if (exception == null) {
                        log.info("recieved new metadata/ \n" +
                                "Key :" + producerRecord.key() + "\n" +
                                "Topic :" + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp());
                    } else {
                        log.error("Error While sending", exception);
                    }
                }
            });
        }
        //Flush and Close the Producer -sync
        producer.flush();

        //flush and close
        producer.close();
    }
}
