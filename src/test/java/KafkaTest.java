import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Properties;

public class KafkaTest {
    @Test
    public void kafkaProducer() throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "bg1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /*String path = this.getClass().getClassLoader().getResource("data.txt").getPath();
        List<String> lines = FileUtils.readLines(new File(path));*/
        for (int i = 0; i < 60; i++) {
            int userId = RandomUtils.nextInt(1, 10);
            int productId = RandomUtils.nextInt(1, 1000);
            String data = userId + "," + productId + "," + System.currentTimeMillis() + "," + 1;
            producer.send(new ProducerRecord<String, String>("con","order", data));
            Thread.sleep(1000);
        }
        producer.close();
    }

    @Test
    public void kafkaConsumer() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "bg1:9092");
        properties.put("group.id", "groupId");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset","latest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("con"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("Consumer Record:(%s, %s, %d, %d)",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset()));
                Thread.sleep(5000);
            }
        }
    }

    @Test
    public void date(){
        System.out.println(new Timestamp(System.currentTimeMillis()).toString());
    }
}
