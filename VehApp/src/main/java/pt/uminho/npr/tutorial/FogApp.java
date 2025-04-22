package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.FogToRsuMessage;
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
    private static final long   COMMAND_TTL_OFFSET_MS     = 5_000 * TIME.MILLI_SECOND;

    private final AtomicLong hazardSeq     = new AtomicLong();
    private final AtomicLong infoSeq       = new AtomicLong();
    private final AtomicLong speedSeq      = new AtomicLong();
    private final AtomicLong routeSeq      = new AtomicLong();
    private final AtomicLong congestionSeq = new AtomicLong();

    private final Map<String, VehicleToVehicle> vehicleStates = new HashMap<>();
    private final Set<String> seenRsuMessages = new HashSet<>();
    private final Set<String> seenHazards     = new HashSet<>();
    private final Set<String> seenInfos       = new HashSet<>();
    private final Set<String> seenSpeeds      = new HashSet<>();
    private final Set<String> seenRoutes      = new HashSet<>();
    private final Set<String> seenCongestions = new HashSet<>();
    private final Set<String> seenAcks        = new HashSet<>();

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
        LOG_INFO("FOG APPLICATION STARTED AND AD-HOC MODULE ENABLED");
        scheduleTick();
    }

    @Override
    public void onShutdown() {
        LOG_INFO("FOG APPLICATION SHUTTING DOWN AND AD-HOC MODULE DISABLED");
        printSeenIds();
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) {
        LOG_DEBUG("PROCESSING TICK EVENT: PURGING STATES AND GENERATING COMMANDS");
        purgeVehicleStates();
        printVehicleStates();
        generateCommands();
        scheduleTick();
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage incoming) {
        V2xMessage msg = incoming.getMessage();
        String type = msg.getClass().getSimpleName();
        // LOG_DEBUG("MESSAGE RECEIVED OF TYPE: " + type);

        if (msg instanceof RsuToFogMessage rtf && seenRsuMessages.add(rtf.getUniqueId())) {
            // LOG_INFO("RECEIVED NEW RSU-TO-FOG MESSAGE ID: " + rtf.getUniqueId());
            V2xMessage inner = rtf.getInnerMessage();
            if (inner instanceof VehicleToVehicle v2v) {
                storeVehicleState(v2v);
            } else if (inner instanceof VehicleToRsuACK ack) {
                handleAck(ack);
            } else {
                LOG_DEBUG("IGNORED UNKNOWN INNER MESSAGE TYPE: " + inner.getClass().getSimpleName());
            }
        } else {
            LOG_DEBUG("IGNORED MESSAGE OR DUPLICATE MESSAGE TYPE: " + type);
        }
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No-op */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No-op */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No-op */ }

    private void storeVehicleState(VehicleToVehicle v2v) {
        vehicleStates.put(v2v.getSenderId().toUpperCase(Locale.ROOT), v2v);
        LOG_INFO("STORED VEHICLE STATE FOR VEHICLE: " + v2v.getSenderId().toUpperCase(Locale.ROOT));
    }

    private void handleAck(VehicleToRsuACK ack) {
        if (seenAcks.add(ack.getUniqueId())) {
            LOG_INFO("RECEIVED NEW VEHICLE ACK ID: " + ack.getUniqueId());
        } else {
            LOG_DEBUG("DUPLICATE VEHICLE ACK IGNORED ID: " + ack.getUniqueId());
        }
    }

    private void purgeVehicleStates() {
        long now = getOs().getSimulationTime();
        int before = vehicleStates.size();
        vehicleStates.entrySet().removeIf(
            e -> now - e.getValue().getTimeStamp() > VEHICLE_STATE_TTL_MS
        );
        LOG_DEBUG("PURGED VEHICLE STATES. BEFORE: " + before + ", AFTER: " + vehicleStates.size());
    }

    private void printVehicleStates() {
        LOG_INFO("VEHICLE_STATES : SIZE = " + vehicleStates.size());
        vehicleStates.values().stream()
            .sorted(Comparator.comparing(v -> v.getSenderId().toUpperCase()))
            .forEach(v2v -> {
                LOG_INFO("VEHICLE_STATE_ENTRY : VEHICLE_ID: " + v2v.getSenderId().toUpperCase() +
                        " | SPEED: " + String.format("%.2f", v2v.getSpeed()) +
                        " | POSITION: " + v2v.getPosition() +
                        " | TIMESTAMP: " + v2v.getTimeStamp());
            });
    }

    private void printSeenIds() {
        Map<String, Set<String>> collections = Map.of(
            "RSU_TO_FOG_IDS",   seenRsuMessages,
            "HAZARD_IDS",       seenHazards,
            "INFO_IDS",         seenInfos,
            "SPEED_IDS",        seenSpeeds,
            "ROUTE_IDS",        seenRoutes,
            "CONGESTION_IDS",   seenCongestions,
            "ACK_IDS",          seenAcks
        );
        collections.forEach((label, set) -> {
            LOG_INFO(label + " : COUNT = " + set.size());
            set.stream().sorted()
               .forEach(id -> LOG_INFO(label.substring(0, label.length() - 4) + "_ID : " + id));
        });
    }

    private void generateCommands() {
        LOG_DEBUG("GENERATING COMMANDS BASED ON CURRENT VEHICLE STATES");
        Collection<VehicleToVehicle> states = vehicleStates.values();
        Set<String> hazardIds = detectHazards(states);
        double avgSpeed = averageSpeed(states);
        int count = states.size();

        if (!hazardIds.isEmpty()) {
            hazardIds.forEach(this::sendHazardAlert);
        } else if (avgSpeed < 5.0) {
            sendSpeedRecommendation();
        } else if (count > 20) {
            sendRouteSuggestion();
        } else if (avgSpeed < 15.0) {
            sendCongestionWarning();
        } else {
            sendTrafficInformation(avgSpeed);
        }
    }

    private Set<String> detectHazards(Collection<VehicleToVehicle> states) {
        Set<String> hazards = new HashSet<>();
        List<VehicleToVehicle> list = new ArrayList<>(states);
        for (int i = 0; i < list.size(); i++) {
            for (int j = i + 1; j < list.size(); j++) {
                VehicleToVehicle a = list.get(i), b = list.get(j);
                double d  = distance(a.getPosition(), b.getPosition());
                double dv = Math.abs(a.getSpeed() - b.getSpeed());
                if (d < 10.0 && dv > 2.0) {
                    hazards.add(a.getSenderId().toUpperCase(Locale.ROOT));
                    hazards.add(b.getSenderId().toUpperCase(Locale.ROOT));
                }
            }
        }
        // LOG_DEBUG("HAZARD DETECTION COMPLETED: " + hazards.size() + " HAZARDS FOUND");
        return hazards;
    }

    private void sendHazardAlert(String vehId) {
        String id = "F2RH" + hazardSeq.getAndIncrement();
        if (seenHazards.add(id)) {
            sendCommand(id, "HAZARD_ALERT", vehId, "HAZARD_DETECTED");
        }
    }

    private void sendTrafficInformation(double avg) {
        String id = "F2RT" + infoSeq.getAndIncrement();
        if (seenInfos.add(id)) {
            sendCommand(id, "TRAFFIC_INFORMATION", "VEH_1", String.format("AVG_SPEED_%.2f", avg));
        }
    }

    private void sendSpeedRecommendation() {
        String id = "F2RS" + speedSeq.getAndIncrement();
        if (seenSpeeds.add(id)) {
            sendCommand(id, "SPEED_RECOMMENDATION", "VEH_5", "INCREASE_SPEED");
        }
    }

    private void sendRouteSuggestion() {
        String id = "F2RR" + routeSeq.getAndIncrement();
        if (seenRoutes.add(id)) {
            sendCommand(id, "ROUTE_SUGGESTION", "VEH_3", "ALTERNATE_ROUTE");
        }
    }

    private void sendCongestionWarning() {
        String id = "F2RC" + congestionSeq.getAndIncrement();
        if (seenCongestions.add(id)) {
            sendCommand(id, "CONGESTION_WARNING", "VEH_0", "HIGH_CONGESTION");
        }
    }

    private void sendCommand(String id, String type, String target, String cmd) {
        long now = getOs().getSimulationTime();
        FogToRsuMessage msg = new FogToRsuMessage(
            newRouting(),
            id,
            now,
            now + COMMAND_TTL_OFFSET_MS,
            type,
            getOs().getId().toUpperCase(),
            "ALL",
            target,
            cmd
        );
        getOs().getAdHocModule().sendV2xMessage(msg);
        // LOG_INFO("SENT COMMAND " + type + " ID: " + id + " TARGET: " + target);
    }

    private double averageSpeed(Collection<VehicleToVehicle> states) {
        return states.isEmpty()
               ? 0.0
               : states.stream().mapToDouble(VehicleToVehicle::getSpeed).average().orElse(0.0);
    }

    private double distance(GeoPoint a, GeoPoint b) {
        double R = 6_371_000;
        double dLat = Math.toRadians(b.getLatitude() - a.getLatitude());
        double dLon = Math.toRadians(b.getLongitude() - a.getLongitude());
        double sLat = Math.sin(dLat / 2);
        double sLon = Math.sin(dLon / 2);
        double h = sLat * sLat
                 + Math.cos(Math.toRadians(a.getLatitude()))
                 * Math.cos(Math.toRadians(b.getLatitude()))
                 * sLon * sLon;
        return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
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

    private void LOG_INFO(String message) {
        getLog().infoSimTime(this, "[" + getOs().getId().toUpperCase() + "] [INFO]  " + message);
    }

    private void LOG_DEBUG(String message) {
        getLog().debugSimTime(this, "[" + getOs().getId().toUpperCase() + "] [DEBUG] " + message);
    }
}