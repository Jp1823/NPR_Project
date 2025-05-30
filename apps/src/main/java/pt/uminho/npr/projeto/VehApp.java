package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.interactions.vehicle.VehicleLaneChange.VehicleLaneChangeMode;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.lib.util.scheduling.EventProcessor;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.*;
import pt.uminho.npr.projeto.records.NodeRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

// Vehicle application for V2X communication and event handling in Eclipse MOSAIC
public final class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    // Configuration constants
    private static final long BEACON_PERIOD_MS = 100 * TIME.MILLI_SECOND;
    private static final long CLEAN_THRESHOLD_MS = 1000 * TIME.MILLI_SECOND;
    private static final int TX_POWER_DBM = 23;
    private static final double TX_RANGE_M = 150.0;
    private static final double RSU_RANGE_M = 150.0;
    private static final int INITIAL_TTL = 10;
    private static final long EVENT_DURATION_NS = 30_000 * TIME.MILLI_SECOND;
    private static final long ACCIDENT_DURATION_NS = 30_000 * TIME.MILLI_SECOND;
    private static final long RESUME_ACCEL_DURATION_NS = 1_000 * TIME.MILLI_SECOND;

    // Static RSU positions
    private static final Map<String, GeoPoint> STATIC_RSUS = Map.ofEntries(
        Map.entry("rsu_0",  GeoPoint.latLon(52.451033, 13.295327, 0)),
        Map.entry("rsu_1",  GeoPoint.latLon(52.451406, 13.298062, 0)),
        Map.entry("rsu_2",  GeoPoint.latLon(52.452119, 13.301154, 0)),
        Map.entry("rsu_3",  GeoPoint.latLon(52.452153, 13.304256, 0)),
        Map.entry("rsu_4",  GeoPoint.latLon(52.450304, 13.301368, 0)),
        Map.entry("rsu_5",  GeoPoint.latLon(52.450268, 13.304328, 0)),
        Map.entry("rsu_6",  GeoPoint.latLon(52.448599, 13.305743, 0)),
        Map.entry("rsu_7",  GeoPoint.latLon(52.448538, 13.302883, 0)),
        Map.entry("rsu_8",  GeoPoint.latLon(52.447641, 13.301310, 0)),
        Map.entry("rsu_9",  GeoPoint.latLon(52.449290, 13.298481, 0)),
        Map.entry("rsu_10", GeoPoint.latLon(52.446686, 13.298421, 0)),
        Map.entry("rsu_11", GeoPoint.latLon(52.446557, 13.294261, 0)),
        Map.entry("rsu_12", GeoPoint.latLon(52.448404, 13.295637, 0)),
        Map.entry("rsu_13", GeoPoint.latLon(52.449206, 13.292616, 0))
    );

    private MessageRouting broadcastRouting;

    // Vehicle state
    private final Map<String, NodeRecord> neighborsGraph = new HashMap<>();
    
    private final Set<Integer> processedCams   = new HashSet<>();
    private final Set<Integer> processedEvents = new HashSet<>();
    private AtomicInteger camIdSeq = new AtomicInteger();

    private String vehId;
    private boolean ready = false;
    //private double heading = 0.0;
    //private double speed = 0.0;
    private double preAccidentSpeed = 0.0;
    private boolean inAccident = false;
    private boolean pendingBrake = false;
    private boolean pendingLaneChange = false;

    @Override
    public void onStartup() {
        // Initialize vehicle ID
        vehId = getOs().getId();

        // Enable ad-hoc communication module
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER_DBM)
                .distance(TX_RANGE_M)
                .create()
        );

        // Initialize broadcast routing
        broadcastRouting = getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();

        scheduleEvent();
        logInfo("VEHICLE_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        getOs().getAdHocModule().disable();
        logInfo("VEHICLE_SHUTDOWN");
    }

    @Override
    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                                @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData cur) {
        // Update vehicle state
        //heading = cur.getHeading().doubleValue();
        //speed = cur.getSpeed();
        ready = true;

        // Handle pending events
        if (pendingBrake && !inAccident) {
            inAccident = true;
            preAccidentSpeed = cur.getSpeed();
            logInfo("APPLYING EMERGENCY BRAKE FOR " +
                    (ACCIDENT_DURATION_NS / TIME.MILLI_SECOND) + " MS");

            getOs().changeSpeedWithInterval(0.0, ACCIDENT_DURATION_NS);
            long resumeTime = getOs().getSimulationTime() + ACCIDENT_DURATION_NS;
            getOs().getEventManager().addEvent(resumeTime, new EventProcessor() {
                @Override
                public void processEvent(Event event) {
                    logInfo("RESUMING SPEED TO " + preAccidentSpeed);
                    getOs().changeSpeedWithInterval(preAccidentSpeed, RESUME_ACCEL_DURATION_NS);
                    inAccident = false;
                }
            });
            pendingBrake = false;
        }

        if (pendingLaneChange) {
            logInfo("LANE CLOSURE EVENT RECEIVED: INITIATING LANE CHANGE");
            getOs().changeLane(VehicleLaneChangeMode.TO_RIGHT, EVENT_DURATION_NS);
            pendingLaneChange = false;
        }
    }

    @Override
    public void processEvent(Event event) {
        long now = getOs().getSimulationTime();

        if (ready) {
            sendCam(now);
        }
        purgeStale(now);
        scheduleEvent();
    }

    private void scheduleEvent() {
        // Schedule the next event
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + BEACON_PERIOD_MS,
            this
        );
    }

    private void purgeStale(long now) {
        // Remove stale neighbor entries
        neighborsGraph.entrySet().removeIf(
            e -> now - e.getValue().getCreationTimestamp() > CLEAN_THRESHOLD_MS
        );
    }

    private void sendCam(long now) {

        CamMessage v2v = new CamMessage(
            broadcastRouting,
            camIdSeq.getAndIncrement(),
            vehId,
            now,
            INITIAL_TTL,
            getOs().getPosition(),
            neighborsGraph
        );
        getOs().getAdHocModule().sendV2xMessage(v2v);
    }

    // Message handling

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();
        if (msg instanceof CamMessage cam) {
            handleCamReceived(cam);

        } else if (msg instanceof EventMessage event && event.hasNextHop() && event.getNextHop().equals(vehId) && processedEvents.add(event.getId())) {
            // Process event message if it is directed to this vehicle and not already processed
            logInfo("EVENT MESSAGE RECEIVED: EVENT_ID =" + event.getId() +
                    " | VEHICLE_TARGET = " + event.getTarget() +
                    " | EVENT_TYPE = " + event.getSimpleClassName());
            handleEventMessage(event);

        } else if (msg instanceof EventACK ack && ack.hasNextHop() && ack.getNextHop().equals(vehId)) {
            // Process the ack if it is directed to this vehicle
            logInfo("EVENT ACK RECEIVED:" +
                    " | ACK_ID = " + ack.getId() +
                    " | NEXT_HOP = " + ack.getNextHop());
            handleAckReceived(ack);
        }
    }

    private void handleCamReceived(CamMessage cam) {
        if (!processedCams.add(cam.getId())) return;

        String sender = cam.getVehId();
        GeoPoint senderPos = cam.getPosition();

        double distSelf   = getOs().getPosition().distanceTo(senderPos);
        double distToRsu  = computeRsuDistance();
        boolean reachableToRsu = distToRsu <= RSU_RANGE_M;

        // Process neighbor information
        List<String> reachableNeighbors = new ArrayList<>(cam.getNeighborsGraph().keySet());
        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = cam.getNeighborsGraph().get(id);
                return nr != null && nr.getDistanceFromVehicle() <= TX_RANGE_M;
            })
            .toList();

        // Update neighbor graph
        NodeRecord recSelf = new NodeRecord(
            distSelf,
            distToRsu,
            reachableToRsu,
            reachableNeighbors,
            directNeighbors,
            cam.getTimestamp()
        );
        neighborsGraph.put(sender, recSelf);

        cam.getNeighborsGraph().forEach((id, rec) -> {
            NodeRecord existing = neighborsGraph.get(id);
            if (existing == null || rec.getCreationTimestamp() > existing.getCreationTimestamp()) {
                neighborsGraph.put(id, rec);
            }
        });

        // Forward message if TTL allows
        if (cam.getTimeToLive() > 1) {
            CamMessage fwd = new CamMessage(
                broadcastRouting,
                cam.getId(),
                cam.getVehId(),
                cam.getTimestamp(),
                cam.getTimeToLive() - 1,
                senderPos,
                cam.getNeighborsGraph()
            );
            getOs().getAdHocModule().sendV2xMessage(fwd);
        }
    }

    private void handleEventMessage(EventMessage event) {
        
        if(event.getTarget().equals(vehId)) {
            processEvent(event);
        } else {
            forwardEvent(event);
        }
    }

    private void processEvent(EventMessage event) {

        // If the event is expired, do not process it
        if (event.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "EVENT_NOT_PROCESSED : UNIQUE_ID: %d | VEHICLE_TARGET: %s | EVENT_EXPIRED",
                event.getId(), event.getTarget()
            ));
            return;
        }

        logInfo(String.format(
            "FINAL TARGET REACHED | PROCESSING EVENT : UNIQUE_ID = %d", 
            event.getId()
        ));
        if(event instanceof AccidentEvent) {
            logInfo("ACCIDENT EVENT RECEIVED: SCHEDULING EMERGENCY BRAKE");
            pendingBrake = true;
        } else {
            logInfo("UNKNOWN EVENT TYPE: " + event.getSimpleClassName());
        }

        // Send acknowledgement back to the source
        sendAcknowledgement(event);
    }

    private void sendAcknowledgement(EventMessage event) {
        
        long now = getOs().getSimulationTime();

        // Build reverse path for ACK
        List<String> forwardingTrail = event.getForwardingTrail();
        forwardingTrail.removeLast(); // Remove the last hop (this vehicle)

        EventACK ack = new EventACK(
            broadcastRouting,
            event.getId(),
            now,
            now + (10_000 * TIME.MILLI_SECOND),
            forwardingTrail
        );

        getOs().getAdHocModule().sendV2xMessage(ack);
        logInfo(String.format(
            "ACK SENT: EVENT_ID = %d | NEXT_HOP = %s", 
            ack.getId(), ack.getNextHop()
        ));
    }

    private void forwardEvent(EventMessage event) {

        // If the event is expired, do not forward it
        if (event.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "EVENT_NOT_FORWARDED : UNIQUE_ID: %d | VEHICLE_TARGET: %s | EVENT_EXPIRED",
                event.getId(), event.getTarget()
            ));
            return;
        }

        // Select next hop for forwarding
        String target = event.getTarget();
        String nextHop = null;
        
        NodeRecord rec = neighborsGraph.get(target);
        if (rec != null && rec.getDistanceFromVehicle() <= TX_RANGE_M) {
            nextHop = target;
        } else {
            nextHop = selectNextHop(target);
        }

        // If no next hop is found, do not forward the event
        if (nextHop == null) {
            logInfo(String.format(
                "EVENT_NOT_FORWARDED : UNIQUE_ID: %s | VEHICLE_TARGET: %s | NO_NEXT_HOP_FOUND",
                event.getId(), target
            ));
            return;
        }

        // Add the next hop to the forwarding trail
        List<String> forwardingTrail = event.getForwardingTrail();
        forwardingTrail.add(nextHop);
        logInfo(vehId + " | FORWARDING TRAIL SIZE: " + forwardingTrail.size());

        // Copy the message to forward the event
        EventMessage eventCopy = new AccidentEvent(
            broadcastRouting,
            event.getId(),
            event.getTimestamp(),
            event.getExpiryTimestamp(),
            event.getTarget(),
            forwardingTrail,
            ((AccidentEvent) event).getSeverity()
        );

        // Send the event message
        getOs().getAdHocModule().sendV2xMessage(eventCopy);
        logInfo(String.format(
            "EVENT_FORWARDED_TO_VEHICLE : UNIQUE_ID: %d | VEHICLE_TARGET: %s | NEXT_HOP: %s",
            event.getId(), target, nextHop
        ));
    }

    private void handleAckReceived(EventACK ack) {

        // If the ACK is expired, do not process it
        if (ack.getExpiryTimestamp() < getOs().getSimulationTime()) {
            logInfo(String.format(
                "ACK_NOT_PROCESSED : UNIQUE_ID: %d | ACK_EXPIRED",
                ack.getId()
            ));
            return;
        }

        // Remove the last hop (this vehicle)
        List<String> checklist = ack.getChecklist();
        checklist.removeLast(); 
        logInfo(vehId + " | check list SIZE: " + checklist.size());

        // Copy the ACK message to forward it
        EventACK ackCopy = new EventACK(
            broadcastRouting,
            ack.getId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            checklist
        );

        // Send the ACK message copy
        getOs().getAdHocModule().sendV2xMessage(ackCopy);
        logInfo(String.format(
            "ACK SENT: EVENT_ID = %d | NEXT_HOP = %s", 
            ackCopy.getId(), ackCopy.getNextHop()
        ));
    }

    private String selectNextHop(String dst) {
        // Find direct neighbors within transmission range
        List<String> direct = neighborsGraph.entrySet().stream()
            .filter(e -> e.getValue().getDistanceFromVehicle() <= TX_RANGE_M)
            .map(Map.Entry::getKey)
            .filter(n -> !n.equalsIgnoreCase(vehId))
            .toList();

        if (direct.isEmpty()) {
            return null;
        }

        // Prefer two-hop neighbors that can reach the destination
        List<String> twoHop = direct.stream()
            .filter(n -> neighborsGraph.get(n).getReachableNeighbors().contains(dst))
            .toList();
        if (!twoHop.isEmpty()) {
            Optional<String> minTwoHop = twoHop.stream()
                .min(Comparator.comparingDouble(n -> neighborsGraph.get(n).getDistanceFromVehicle()));
            if (minTwoHop.isPresent()) {
                return minTwoHop.get();
            }
        }

        // Select neighbor with minimum hop count
        String bestByHops = direct.stream()
            .min(Comparator.comparingInt(n -> hopCountRestricted(n, dst, direct)))
            .orElse(null);
        if (bestByHops != null && hopCountRestricted(bestByHops, dst, direct) < Integer.MAX_VALUE) {
            return bestByHops;
        }

        // Fallback to neighbor with most connections
        return direct.stream()
            .max(Comparator.comparingInt(n -> neighborsGraph.get(n).getReachableNeighbors().size()))
            .orElse(null);
    }

    private int hopCountRestricted(String start, String dst, List<String> directCandidates) {
        // Compute shortest path to destination within allowed nodes
        if (start.equalsIgnoreCase(dst)) {
            return 0;
        }

        Queue<String> queue = new LinkedList<>();
        Map<String, Integer> dist = new HashMap<>();
        queue.offer(start);
        dist.put(start, 0);

        Set<String> allowed = new HashSet<>(directCandidates);
        allowed.add(dst);

        while (!queue.isEmpty()) {
            String cur = queue.poll();
            int hops = dist.get(cur);
            if (cur.equalsIgnoreCase(dst)) {
                return hops;
            }

            NodeRecord rec = neighborsGraph.get(cur);
            if (rec == null) {
                continue;
            }

            for (String nb : rec.getReachableNeighbors()) {
                if (!allowed.contains(nb)) {
                    continue;
                }
                if (dist.putIfAbsent(nb, hops + 1) == null) {
                    queue.offer(nb);
                }
            }
        }
        return Integer.MAX_VALUE;
    }

    private double computeRsuDistance() {
        return getOs().getPosition().distanceTo(STATIC_RSUS.get("rsu_0"));
    }

    private Map.Entry<String, GeoPoint> findClosestRsu(GeoPoint vehiclePosition) {
        // Find the RSU with the minimum distance to the vehicle
        return STATIC_RSUS.entrySet().stream()
            .min(Comparator.comparingDouble(entry -> 
                vehiclePosition.distanceTo(entry.getValue())))
            .orElse(null);
    }

    private List<String> getReachableRsus(GeoPoint vehiclePosition) {
        // Get IDs of RSUs within communication range
        return STATIC_RSUS.entrySet().stream()
            .filter(entry -> vehiclePosition.distanceTo(entry.getValue()) <= RSU_RANGE_M)
            .map(Map.Entry::getKey)
            .toList();
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { /* No action required */ }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { /* No action required */ }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { /* No action required */ }
    
    // Logging
    private void logInfo(String msg) {
        getLog().infoSimTime(this, "[" + vehId + "] [info] " + msg);
    }
}