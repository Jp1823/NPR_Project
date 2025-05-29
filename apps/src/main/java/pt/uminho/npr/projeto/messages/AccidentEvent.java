package pt.uminho.npr.projeto.messages;

import java.util.List;

import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;

public class AccidentEvent extends EventMessage {

    private final String severity;

    public AccidentEvent(MessageRouting routing,
                         int eventId,
                         long timestamp,
                         long expiryTimestamp,
                         String target,
                         List<String> forwardingTrail,
                         String severity) {
        super(routing, eventId, timestamp, expiryTimestamp, target, forwardingTrail);
        this.severity = severity;
    }

    public String getSeverity() {
        return severity;
    }

    @Override
    public String toString() {
        return "ACCIDENT_EVENT :" +
                " | SEVERITY: " + severity +
                " | " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AccidentEvent)) return false;
        AccidentEvent that = (AccidentEvent) o;
        return severity.equals(that.severity) &&
               super.equals(that);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + severity.hashCode();
        return result;
    }
}
