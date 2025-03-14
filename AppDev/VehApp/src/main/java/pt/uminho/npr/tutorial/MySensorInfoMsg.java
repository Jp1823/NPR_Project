package pt.uminho.npr.tutorial;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.EncodedPayload;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.SerializationUtils;

/**
 * MySensorInfoMsg: mensagem customizada enviada por um sensor de tráfego.
 */
public class MySensorInfoMsg extends V2xMessage {

    private final EncodedPayload payload;
    private final long timeStamp;
    private final String sensorName;
    private final GeoPoint sensorPosition;
    private final String sensorData;

    public MySensorInfoMsg(MessageRouting routing,
                           long time,
                           String sensorName,
                           GeoPoint sensorPos,
                           String data) {
        super(routing);
        this.timeStamp = time;
        this.sensorName = sensorName;
        this.sensorPosition = sensorPos;
        this.sensorData = data;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // Exemplo de serialização de campos:
            dos.writeLong(timeStamp);
            dos.writeUTF(sensorName);
            SerializationUtils.encodeGeoPoint(dos, sensorPosition);
            dos.writeUTF(sensorData);

            // Cria payload final
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("Erro ao serializar MySensorInfoMsg", e);
        }
    }

    @Nonnull
    @Override
    public EncodedPayload getPayload() {
        return payload;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getSensorName() {
        return sensorName;
    }

    public GeoPoint getSensorPosition() {
        return sensorPosition;
    }

    public String getSensorData() {
        return sensorData;
    }

    @Override
    public String toString() {
        return "MySensorInfoMsg{" +
                "timeStamp=" + timeStamp +
                ", sensorName='" + sensorName + '\'' +
                ", sensorPosition=" + sensorPosition +
                ", sensorData='" + sensorData + '\'' +
                '}';
    }
}
