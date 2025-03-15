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

public class FogInfoMsg extends V2xMessage {
    private final EncodedPayload payload;
    private final long timeStamp;
    private final String fogName;
    private final GeoPoint fogPosition;
    private final String analysisResult;

    public FogInfoMsg(MessageRouting routing,
                      long time,
                      String name,
                      GeoPoint position,
                      String analysisText) {
        super(routing);
        this.timeStamp = time;
        this.fogName = name;
        this.fogPosition = position;
        this.analysisResult = analysisText;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(timeStamp);
            dos.writeUTF(fogName);
            SerializationUtils.encodeGeoPoint(dos, fogPosition);
            dos.writeUTF(analysisResult);
            payload = new EncodedPayload(baos.toByteArray(), baos.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR SERIALIZING FOGINFOMSG", e);
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

    public String getFogName() {
        return fogName;
    }

    public GeoPoint getFogPosition() {
        return fogPosition;
    }

    public String getAnalysisResult() {
        return analysisResult;
    }

    @Override
    public String toString() {
        return "FOGINFOMSG{TIMESTAMP=" + timeStamp + ", FOGNAME='" + fogName + "', FOGPOS=" + fogPosition +
               ", ANALYSIS='" + analysisResult + "'}";
    }
}