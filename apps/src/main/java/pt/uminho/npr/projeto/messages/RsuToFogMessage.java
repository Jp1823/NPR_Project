package pt.uminho.npr.projeto.messages;

import java.io.*;
import java.util.*;
import javax.annotation.Nonnull;

import org.eclipse.mosaic.lib.objects.v2x.*;

public final class RsuToFogMessage extends V2xMessage {

    private final long timestamp;
    private final String rsuIdentifier;
    private final V2xMessage innerMessage;

    public RsuToFogMessage(MessageRouting routing,
                           long timestamp,
                           String rsuIdentifier,
                           V2xMessage innerMessage) {
        super(routing);
        this.timestamp          = timestamp;
        this.rsuIdentifier      = Objects.requireNonNull(rsuIdentifier);
        this.innerMessage       = Objects.requireNonNull(innerMessage);
    }

    @Nonnull @Override public EncodedPayload getPayload() {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(buf)) {
            out.writeLong(timestamp);
            out.writeUTF(rsuIdentifier);

            byte[] inner = innerMessage.getPayload().getBytes();
            out.writeInt(inner.length);
            out.write(inner);

            return new EncodedPayload(buf.toByteArray(), buf.size());
        } catch (IOException e) {
            throw new RuntimeException("ERROR ENCODING RSU_TO_FOG_MESSAGE", e);
        }
    }

    public long getTimestamp() { return timestamp; }
    public String getRsuIdentifier() { return rsuIdentifier; }
    public V2xMessage getInnerMessage() { return innerMessage; }

    @Override public String toString() {
        return "RSU_TO_FOG_MESSAGE :" +
               " | RSU_IDENTIFIER: " + rsuIdentifier +
               " | INNER_MESSAGE_TYPE: " + innerMessage.getClass().getSimpleName() +
               " | TIMESTAMP: " + timestamp;
    }
}
