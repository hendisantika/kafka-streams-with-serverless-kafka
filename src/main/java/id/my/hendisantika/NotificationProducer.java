package id.my.hendisantika;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * Project : kafka-streams-with-serverless-kafka
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 5/23/24
 * Time: 07:13
 * To change this template use File | Settings | File Templates.
 */
public class NotificationProducer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendNotification(NotificationDTO notificationDTO, Properties propertiesFromFile) {
        try {
            Properties kafkaProducerProperties = getKafkaProducerProperties(propertiesFromFile);

            String topicName = propertiesFromFile.get("kafka.notification.topic").toString();

            try (var producer = new KafkaProducer<String, String>(kafkaProducerProperties)) {

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                        topicName,
                        notificationDTO.getNotificationId().toString(),
                        objectMapper.writeValueAsString(notificationDTO));

                producer.send(producerRecord);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties getKafkaProducerProperties(Properties propertiesFromFile) {
        var kafkaProducerProperties = new Properties();

        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propertiesFromFile.get("kafka.bootstrap.servers"));
        kafkaProducerProperties.put("sasl.mechanism", propertiesFromFile.get("kafka.sasl.mechanism"));
        kafkaProducerProperties.put("security.protocol", propertiesFromFile.get("kafka.security.protocol"));
        kafkaProducerProperties.put("sasl.jaas.config", propertiesFromFile.get("kafka.sasl.jaas.config"));
        return kafkaProducerProperties;
    }
}
