package id.my.hendisantika;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by IntelliJ IDEA.
 * Project : kafka-streams-with-serverless-kafka
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 5/23/24
 * Time: 07:12
 * To change this template use File | Settings | File Templates.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NotificationDTO implements Serializable {
    private UUID notificationId;
    private NotificationType notificationType;
    private String message;
    private String to;
}
