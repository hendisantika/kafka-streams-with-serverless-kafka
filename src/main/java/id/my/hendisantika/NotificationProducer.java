package id.my.hendisantika;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
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
}
