import lesson.LoggingConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Application {
    public static Logger log = LoggerFactory.getLogger("appl");
    public static final String HOST = "localhost:9091";
    public static final String HOST_PRODUCER = "localhost:9092";
    public static final int numPartitions = 1;
    public static final short replicationFactor = 1;

    public static final Map<String, Object> consumerConfig = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
            ConsumerConfig.GROUP_ID_CONFIG, "some-java-consumer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    public static final Map<String, Object> producerPropsTr = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_PRODUCER,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "id_transaction");

    public static final Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST_PRODUCER,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> bootstrapServersConfig = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        Admin admin = Admin.create(bootstrapServersConfig);
        admin.createTopics(Stream.of("topic1", "topic2")
                .map(it -> new NewTopic(it, numPartitions, replicationFactor))
                .collect(Collectors.toList()));
        try (
                var producerTransactional = new KafkaProducer<String, String>(producerPropsTr);
                var producer = new KafkaProducer<String, String>(producerProps);
                var consumerRC = new LoggingConsumer("ReadCommitted", "topic1", consumerConfig, true);
//                var consumerRUnC = new LoggingConsumer("ReadUncommitted", "topic1", consumerConfig, false)
        ) {

            producerTransactional.initTransactions();

            log.info("beginTransaction");
            producerTransactional.beginTransaction();
            sendToTopics(producerTransactional, 5, "topic1", "topic2");
            log.info("commitTransaction");
            producerTransactional.commitTransaction(); // consumerRC получит 1,2,3 ...
            log.info("beginTransaction");
            producerTransactional.beginTransaction();
            sendToTopics(producerTransactional, 2, "topic1", "topic2");
            log.info("abortTransaction");
            producerTransactional.abortTransaction();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private static void sendToTopics(KafkaProducer<String, String> producer, Integer times, String... topicNames) {
        for (int i = 0; i < times; i++) {
            for (String s :
                    topicNames) {
                producer.send(new ProducerRecord<>(s, "" + i));
            }
        }

    }
}
