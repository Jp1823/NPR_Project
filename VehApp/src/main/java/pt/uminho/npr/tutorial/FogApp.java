package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.FogToRsuMessage;
import pt.uminho.npr.tutorial.Messages.FogEventMessage;
import pt.uminho.npr.tutorial.Messages.RsuToFogMessage;
import pt.uminho.npr.tutorial.Messages.VehicleToRsuACK;
import pt.uminho.npr.tutorial.Messages.VehicleToVehicle;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public final class FogApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private static final long   TICK_MS                   = 1_000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM              = 23;
    private static final double TX_RANGE_M                = 100.0;
    private static final long   VEHICLE_STATE_TTL_MS      = 5_000 * TIME.MILLI_SECOND;

    private static final double EVENT_PROBABILITY         = 0.05;
    private static final long   EVENT_TTL_MS              = 5_000 * TIME.MILLI_SECOND;
    private static final int    MAX_AFFECTED_VEHICLES     = 3;

    private final AtomicLong eventSeq        = new AtomicLong();
    private final Map<String, VehicleToVehicle> vehicleStates = new HashMap<>();
    private final Set<String> seenRsuMessages = new HashSet<>();
    private final Set<String> seenAcks        = new HashSet<>();
    private final Random random = new Random();

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER_DBM)
                .distance(TX_RANGE_M)
                .create()
        );
        scheduleTick();
    }

    @Override
    public void onShutdown() {
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) {
        purgeVehicleStates();
        maybeGenerateEvent();
        scheduleTick();
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage incoming) {
        V2xMessage msg = incoming.getMessage();
        if (msg instanceof RsuToFogMessage rtf && seenRsuMessages.add(rtf.getUniqueId())) {
            V2xMessage inner = rtf.getInnerMessage();
            if (inner instanceof VehicleToVehicle v2v) {
                storeVehicleState(v2v);
            } else if (inner instanceof VehicleToRsuACK ack) {
                handleAck(ack);
            }
        }
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { }

    private void storeVehicleState(VehicleToVehicle v2v) {
        vehicleStates.put(v2v.getSenderId().toUpperCase(Locale.ROOT), v2v);
    }

    private void handleAck(VehicleToRsuACK ack) {
        seenAcks.add(ack.getUniqueId());
    }

    private void purgeVehicleStates() {
        long now = getOs().getSimulationTime();
        vehicleStates.entrySet().removeIf(
            e -> now - e.getValue().getTimeStamp() > VEHICLE_STATE_TTL_MS
        );
    }

    private void maybeGenerateEvent() {
        if (vehicleStates.isEmpty() || random.nextDouble() >= EVENT_PROBABILITY) {
            return;
        }

        long now = getOs().getSimulationTime();
        String fogId = getOs().getId().toUpperCase(Locale.ROOT);

        boolean accident = random.nextBoolean();
        String eventType = accident ? "ACCIDENT" : "LANE_CLOSURE";

        List<String> allIds = new ArrayList<>(vehicleStates.keySet());
        Collections.shuffle(allIds, random);
        List<String> affected = allIds.subList(0, Math.min(MAX_AFFECTED_VEHICLES, allIds.size()));

        double sumLat = 0, sumLon = 0;
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

        Map<String, String> params = new HashMap<>();
        params.put("affectedVehicles", String.join(",", affected));
        if (accident) {
            String[] levels = { "LOW", "MEDIUM", "HIGH" };
            params.put("severity", levels[random.nextInt(levels.length)]);
        } else {
            params.put("lanesClosed", Integer.toString(1 + random.nextInt(2)));
        }

        for (String target : affected) {
            String id = "EVT-" + eventSeq.getAndIncrement();
            logInfo("GENERATING EVENT " + id + " TYPE " + eventType + " AT " + location + " FOR " + target);
            FogEventMessage ev = new FogEventMessage(
                newRouting(),
                id,
                now,
                now + EVENT_TTL_MS,
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
            ev.getUniqueId(),
            ev.getTimestamp(),
            ev.getExpiryTimestamp(),
            ev.getEventType(),
            ev.getFogSource(),
            "ALL",
            vehicleTarget,
            ev
        );
        getOs().getAdHocModule().sendV2xMessage(msg);
        logInfo("SENT EVENT " + ev.getUniqueId() + " TO " + vehicleTarget);
    }

    private MessageRouting newRouting() {
        return getOs().getAdHocModule()
                      .createMessageRouting()
                      .viaChannel(AdHocChannel.CCH)
                      .topoBroadCast();
    }

    private void scheduleTick() {
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + TICK_MS,
            this
        );
    }

    private void logInfo(String message) {
        getLog().infoSimTime(this,
            "[" + getOs().getId().toUpperCase() + "] [INFO]  " + message);
    }
}