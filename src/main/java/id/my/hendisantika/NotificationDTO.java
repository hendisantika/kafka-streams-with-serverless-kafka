package id.my.hendisantika;

import java.io.Serializable;
import java.util.Objects;
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
public class NotificationDTO implements Serializable {
    private UUID notificationId;
    private NotificationType notificationType;
    private String message;
    private String to;

    public NotificationDTO() {
    }

    public NotificationDTO(UUID notificationId, NotificationType notificationType, String message, String to) {
        this.notificationId = notificationId;
        this.notificationType = notificationType;
        this.message = message;
        this.to = to;
    }

    public UUID getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(UUID notificationId) {
        this.notificationId = notificationId;
    }

    public NotificationType getNotificationType() {
        return notificationType;
    }

    public void setNotificationType(NotificationType notificationType) {
        this.notificationType = notificationType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationDTO that = (NotificationDTO) o;
        return notificationId.equals(that.notificationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(notificationId);
    }

    @Override
    public String toString() {
        return "NotificationDTO{" +
                "notificationId=" + notificationId +
                ", notificationType=" + notificationType +
                ", message='" + message + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}
