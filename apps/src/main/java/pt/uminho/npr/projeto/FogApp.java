package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.app.api.os.ServerOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CellModuleConfiguration;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.objects.v2x.V2xReceiverInformation;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.DATA;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.FogEventMessage;
import pt.uminho.npr.projeto.messages.FogToRsuMessage;
import pt.uminho.npr.projeto.messages.VehicleToRsuACK;
import pt.uminho.npr.projeto.messages.CamMessage;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public final class FogApp extends AbstractApplication<ServerOperatingSystem>
        implements CommunicationApplication {

    private static final long TICK_INTERVAL = 1 * TIME.SECOND;
    private static final long VEH_STATE_TTL = 5 * TIME.SECOND;
    private static final long EVENT_TTL     = 5 * TIME.SECOND;

    private static final double EVENT_PROBABILITY = 0.05;

    private static final Map<String, GeoPoint> STATIC_RSUS = new HashMap<>();
    static {
        STATIC_RSUS.put("rsu_0",  GeoPoint.latLon(52.451033, 13.295327, 0));
        STATIC_RSUS.put("rsu_1",  GeoPoint.latLon(52.451406, 13.298062, 0));
        STATIC_RSUS.put("rsu_2",  GeoPoint.latLon(52.452119, 13.301154, 0));
        STATIC_RSUS.put("rsu_3",  GeoPoint.latLon(52.452153, 13.304256, 0));
        STATIC_RSUS.put("rsu_4",  GeoPoint.latLon(52.450304, 13.301368, 0));
        STATIC_RSUS.put("rsu_5",  GeoPoint.latLon(52.450268, 13.304328, 0));
        STATIC_RSUS.put("rsu_6",  GeoPoint.latLon(52.448599, 13.305743, 0));
        STATIC_RSUS.put("rsu_7",  GeoPoint.latLon(52.448538, 13.302883, 0));
        STATIC_RSUS.put("rsu_8",  GeoPoint.latLon(52.447641, 13.301310, 0));
        STATIC_RSUS.put("rsu_9",  GeoPoint.latLon(52.449290, 13.298481, 0));
        STATIC_RSUS.put("rsu_10", GeoPoint.latLon(52.446686, 13.298421, 0));
        STATIC_RSUS.put("rsu_11", GeoPoint.latLon(52.446557, 13.294261, 0));
        STATIC_RSUS.put("rsu_12", GeoPoint.latLon(52.448404, 13.295637, 0));
        STATIC_RSUS.put("rsu_13", GeoPoint.latLon(52.449206, 13.292616, 0));
    }
    
    private final Map<String, CamMessage> vehicleStates = new HashMap<>();
    private final Set<Integer> seenRsuMessages = new HashSet<>();
    private final Set<Integer> seenAcks        = new HashSet<>();
    private final AtomicLong  eventSeq        = new AtomicLong();
    private final Random      random          = new Random();
    private String fogId;

    @Override
    public void onStartup() {
        
        fogId = getOs().getId();
        // Enable communication module 
        getOs().getCellModule().enable(new CellModuleConfiguration()
            .maxDownlinkBitrate(50 * DATA.MEGABIT)
            .maxUplinkBitrate(50 * DATA.MEGABIT)
        );
        scheduleEvent();
        logInfo("FOG_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        getOs().getCellModule().disable();
        logInfo("FOG_SHUTDOWN");
    }
    
    private void scheduleEvent() {
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + TICK_INTERVAL,
            this
        );
    }
    
    @Override
    public void onMessageReceived(ReceivedV2xMessage incoming) {
        V2xMessage msg = incoming.getMessage();
        V2xReceiverInformation receiverInfo = incoming.getReceiverInformation();

        if (msg instanceof CamMessage v2v) {
            vehicleStates.put(v2v.getSenderId(), v2v);
        } else if (msg instanceof VehicleToRsuACK ack) {
            seenAcks.add(ack.getId());
        }
    }

    @Override
    public void processEvent(Event event) {
        cleanVars();
        maybeGenerateEvent();
        scheduleEvent();
    }
    
    private void cleanVars() {
        purgeVehicleStates();
        //purgeSeenMessages();
        //purgeSeenAcks();
    }

    private void purgeVehicleStates() {
        long now = getOs().getSimulationTime();
        vehicleStates.entrySet().removeIf(
            e -> now - e.getValue().getTimeStamp() > VEH_STATE_TTL
        );
    }

    private void maybeGenerateEvent() {
        if (vehicleStates.isEmpty() || random.nextDouble() >= EVENT_PROBABILITY) {
            return;
        }

        List<String> allIds = new ArrayList<>(vehicleStates.keySet());
        Collections.shuffle(allIds, random);
        List<String> affected = allIds.subList(0, Math.min(1, allIds.size()));

        double sumLat = 0;
        double sumLon = 0;
        for (String vid : affected) {
            GeoPoint p = vehicleStates.get(vid).getPosition();
            sumLat += p.getLatitude();
            sumLon += p.getLongitude();
        }
        GeoPoint location = GeoPoint.latLon(
            sumLat / affected.size(),
            sumLon / affected.size(),
            0
        );

        boolean accident = random.nextBoolean();
        String eventType = accident ? "ACCIDENT" : "LANE_CLOSURE";

        Map<String, String> params = new HashMap<>();
        params.put("affectedVehicles", String.join(",", affected));
        if (accident) {
            String[] levels = { "LOW", "MEDIUM", "HIGH" };
            params.put("severity", levels[random.nextInt(levels.length)]);
        } else {
            params.put("lanesClosed", Integer.toString(1 + random.nextInt(2)));
        }

        long now = getOs().getSimulationTime();

        for (String target : affected) {
            String id = String.format(
                "EVT-%s-%s-%d",
                fogId, target, eventSeq.getAndIncrement()
            );
            logInfo(String.format(
                "EVENT_GENERATED : UNIQUE_ID: %s | EVENT_TYPE: %s", id, eventType
            ));
            FogEventMessage ev = new FogEventMessage(
                newRouting(),
                now,
                now + EVENT_TTL,
                fogId,
                eventType,
                location,
                params
            );
            sendEvent(ev, target);
        }
    }

    private void sendEvent(FogEventMessage ev, String vehicleTarget) {
        FogToRsuMessage msg = new FogToRsuMessage(
            newRouting(),
            ev.getTimestamp(),
            ev.getExpiryTimestamp(),
            ev.getEventType(),
            ev.getFogSource(),
            vehicleTarget,
            ev
        );
        getOs().getCellModule().sendV2xMessage(msg);
        logInfo(String.format(
            "EVENT_SENT : VEHICLE_TARGET: %s", vehicleTarget
        ));
    }

    private MessageRouting newRouting() {
        return getOs().getCellModule()
            .createMessageRouting()
            .destination("rsu_0")
            .topological()
            .build();
    }
    
    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No action required */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No action required */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No action required */ }

    private void logInfo(String message) {
        getLog().infoSimTime(this, "[" + fogId + "] [INFO]  " + message);
    }
}