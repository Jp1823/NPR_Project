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
import java.util.stream.Collectors;

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
    private static final long PRINT_INTERVAL_MS = 30_000 * TIME.MILLI_SECOND;

    // Static RSU positions
    private static final Map<String, GeoPoint> STATIC_RSUS = new HashMap<>();
    static {
        STATIC_RSUS.put("RSU_0", GeoPoint.latLon(52.451033, 13.295327, 0));
        STATIC_RSUS.put("RSU_1", GeoPoint.latLon(52.451406, 13.298062, 0));
        STATIC_RSUS.put("RSU_2", GeoPoint.latLon(52.452119, 13.301154, 0));
        STATIC_RSUS.put("RSU_3", GeoPoint.latLon(52.452153, 13.304256, 0));
        STATIC_RSUS.put("RSU_4", GeoPoint.latLon(52.450304, 13.301368, 0));
        STATIC_RSUS.put("RSU_5", GeoPoint.latLon(52.450268, 13.304328, 0));
        STATIC_RSUS.put("RSU_6", GeoPoint.latLon(52.448599, 13.305743, 0));
        STATIC_RSUS.put("RSU_7", GeoPoint.latLon(52.448538, 13.302883, 0));
        STATIC_RSUS.put("RSU_8", GeoPoint.latLon(52.447641, 13.301310, 0));
        STATIC_RSUS.put("RSU_9", GeoPoint.latLon(52.449290, 13.298481, 0));
        STATIC_RSUS.put("RSU_10", GeoPoint.latLon(52.446686, 13.298421, 0));
        STATIC_RSUS.put("RSU_11", GeoPoint.latLon(52.446557, 13.294261, 0));
        STATIC_RSUS.put("RSU_12", GeoPoint.latLon(52.448404, 13.295637, 0));
        STATIC_RSUS.put("RSU_13", GeoPoint.latLon(52.449206, 13.292616, 0));
    }

    // Vehicle state
    private final Map<String, NodeRecord> neighborGraph = new HashMap<>();
    private final Set<Integer> processedV2V = new HashSet<>();
    private final Set<Integer> processedAck = new HashSet<>();
    private String vehId;
    private boolean ready = false;
    private double heading = 0.0;
    private double speed = 0.0;
    private double preAccidentSpeed = 0.0;
    private boolean inAccident = false;
    private volatile boolean pendingBrake = false;
    private volatile boolean pendingLaneChange = false;
    private long lastPrintTime = 0;

    // Lifecycle methods

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

        scheduleEvent();
        logInfo("vehicle initialization complete");
    }

    @Override
    public void onShutdown() {
        getOs().getAdHocModule().disable();
        logInfo("vehicle shutdown complete");
    }

    @Override
    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                                @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData cur) {
        // Update vehicle state
        heading = cur.getHeading().doubleValue();
        speed = cur.getSpeed();
        ready = true;

        // Handle pending events
        if (pendingBrake && !inAccident) {
            inAccident = true;
            preAccidentSpeed = speed;
            logInfo("accident event received: applying emergency brake for " +
                    (ACCIDENT_DURATION_NS / TIME.MILLI_SECOND) + " ms");

            getOs().changeSpeedWithInterval(0.0, ACCIDENT_DURATION_NS);
            long resumeTime = getOs().getSimulationTime() + ACCIDENT_DURATION_NS;
            getOs().getEventManager().addEvent(resumeTime, new EventProcessor() {
                @Override
                public void processEvent(Event event) {
                    logInfo("resuming speed to " + preAccidentSpeed);
                    getOs().changeSpeedWithInterval(preAccidentSpeed, RESUME_ACCEL_DURATION_NS);
                    inAccident = false;
                }
            });
            pendingBrake = false;
        }

        if (pendingLaneChange) {
            logInfo("lane closure event received: initiating lane change");
            getOs().changeLane(VehicleLaneChangeMode.TO_RIGHT, EVENT_DURATION_NS);
            pendingLaneChange = false;
        }
    }

    // Event handling

    @Override
    public void processEvent(Event event) {
        long now = getOs().getSimulationTime();

        if (ready) {
            sendBeacon(now);
        }
        purgeStale(now);

        if (now - lastPrintTime >= PRINT_INTERVAL_MS) {
            lastPrintTime = now;
        }

        scheduleEvent();
    }

    private void scheduleEvent() {
        // Schedule the next beacon event
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + BEACON_PERIOD_MS,
            this
        );
    }

    private void purgeStale(long now) {
        // Remove stale neighbor entries
        neighborGraph.entrySet().removeIf(
            e -> now - e.getValue().getCreationTimestamp() > CLEAN_THRESHOLD_MS
        );
    }

    // Beacon transmission

    private void sendBeacon(long now) {
        // Create beacon with neighbor graph payload
        Map<String, NodeRecord> payload = new HashMap<>(neighborGraph);
        MessageRouting routing = newRouting();
        CamMessage v2v = new CamMessage(
            routing,
            now,
            vehId,
            getOs().getPosition(),
            heading,
            speed,
            0.0,
            false,
            false,
            false,
            INITIAL_TTL,
            payload
        );
        getOs().getAdHocModule().sendV2xMessage(v2v);
    }

    // Message handling

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();
        if (msg instanceof CamMessage v2v) {
            handleV2V(v2v);
        } else if (msg instanceof RsuToVehicleMessage rtv) {
            logInfo("rsu to vehicle message received: uniqueId=" + rtv.getId() +
                    " rsuSource=" + rtv.getRsuSource() +
                    " vehicleTarget=" + rtv.getVehicleTarget() +
                    " nextHop=" + rtv.getNextHop() +
                    " command=" + rtv.getCommandEvent());
            handleR2V(rtv);
        } else if (msg instanceof VehicleToRsuACK ack) {
            logInfo("vehicle to rsu ack received: uniqueId=" + ack.getId() +
                    " originalId=" + ack.getOriginalMessageId() +
                    " nextHop=" + ack.getNextHop());
            forwardAck(ack);
        }
    }

    private void handleV2V(CamMessage v2v) {
        // Ignore duplicate messages
        if (!processedV2V.add(v2v.getId())) {
            return;
        }

        String sender = v2v.getSenderId();
        GeoPoint senderPos = v2v.getPosition();

        // Calculate distances and reachability
        double distSelf = getOs().getPosition().distanceTo(senderPos);
        double distToRsu = computeRsuDistance(senderPos);
        boolean reachableToRsu = distToRsu <= RSU_RANGE_M;

        // Process neighbor information
        List<String> reachableNeighbors = new ArrayList<>(v2v.getNeighborGraph().keySet());
        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = v2v.getNeighborGraph().get(id);
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
            v2v.getTimeStamp()
        );
        neighborGraph.put(sender, recSelf);

        v2v.getNeighborGraph().forEach((id, rec) -> {
            NodeRecord existing = neighborGraph.get(id);
            if (existing == null || rec.getCreationTimestamp() > existing.getCreationTimestamp()) {
                neighborGraph.put(id, rec);
            }
        });

        // Forward message if TTL allows
        if (v2v.getTimeToLive() > 1) {
            CamMessage fwd = new CamMessage(
                newRouting(),
                v2v.getTimeStamp(),
                v2v.getSenderId(),
                senderPos,
                v2v.getHeading(),
                v2v.getSpeed(),
                v2v.getAcceleration(),
                v2v.isBrakeLightOn(),
                v2v.isLeftTurnSignalOn(),
                v2v.isRightTurnSignalOn(),
                v2v.getTimeToLive() - 1,
                v2v.getNeighborGraph()
            );
            getOs().getAdHocModule().sendV2xMessage(fwd);
        }
    }

    private void handleR2V(RsuToVehicleMessage rtv) {
        // Prevent loops and check if this vehicle is the next hop
        if (rtv.getForwardingTrail().contains(vehId) || !rtv.getNextHop().equalsIgnoreCase(vehId)) {
            return;
        }

        // Handle message for this vehicle
        if (rtv.getVehicleTarget().equalsIgnoreCase(vehId)) {
            logInfo("final target reached, sending ack: uniqueId=" + rtv.getId());
            FogEventMessage ev = rtv.getCommandEvent();
            switch (ev.getEventType()) {
                case "ACCIDENT":
                    logInfo("accident event received: scheduling emergency brake");
                    pendingBrake = true;
                    break;
                case "LANE_CLOSURE":
                    logInfo("lane closure event received: scheduling lane change");
                    pendingLaneChange = true;
                    break;
                default:
                    logInfo("unknown event type: " + ev.getEventType());
            }
            sendAcknowledgement(rtv);
            return;
        }

        // Forward message to destination
        String dst = rtv.getVehicleTarget();
        NodeRecord rec = neighborGraph.get(dst);
        if (rec != null && rec.getDistanceFromVehicle() <= TX_RANGE_M) {
            List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
            trail.add(vehId);
            RsuToVehicleMessage direct = new RsuToVehicleMessage(
                newRouting(),
                rtv.getTimestamp(),
                rtv.getExpiryTimestamp(),
                rtv.getRsuSource(),
                dst,
                dst,
                rtv.getCommandEvent(),
                trail
            );
            getOs().getAdHocModule().sendV2xMessage(direct);
            return;
        }

        // Select next hop for forwarding
        String next = chooseNextHop(dst);
        if (next == null) {
            return;
        }

        logInfo("forwarding rsu to vehicle message: uniqueId=" + rtv.getId() + " nextHop=" + next);
        List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
        trail.add(vehId);
        RsuToVehicleMessage fwd = new RsuToVehicleMessage(
            newRouting(),
            rtv.getTimestamp(),
            rtv.getExpiryTimestamp(),
            rtv.getRsuSource(),
            dst,
            next,
            rtv.getCommandEvent(),
            trail
        );
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }

    private void sendAcknowledgement(RsuToVehicleMessage originalMessage) {
        long currentTime = getOs().getSimulationTime();

        // Build reverse path for ACK
        List<String> backwardPath = new ArrayList<>(originalMessage.getForwardingTrail());
        Collections.reverse(backwardPath);

        if (backwardPath.isEmpty()) {
            logInfo("empty forwarding trail in rsu message");
            return;
        }

        String nextHop = backwardPath.get(0);
        List<String> remainingPath = backwardPath.size() > 1
            ? new ArrayList<>(backwardPath.subList(1, backwardPath.size()))
            : Collections.emptyList();

        VehicleToRsuACK acknowledgement = new VehicleToRsuACK(
            newRouting(),
            originalMessage.getId(),
            currentTime,
            currentTime + (10_000 * TIME.MILLI_SECOND),
            vehId,
            originalMessage.getRsuSource(),
            nextHop,
            remainingPath
        );

        logInfo("sending ack: uniqueId=" + acknowledgement.getId() +
                " originalId=" + originalMessage.getId() +
                " nextHop=" + nextHop);

        processedAck.add(acknowledgement.getId());
        getOs().getAdHocModule().sendV2xMessage(acknowledgement);
    }

    private void forwardAck(VehicleToRsuACK ack) {
        // Ignore duplicate or misrouted ACKs
        if (!processedAck.add(ack.getId()) || !ack.getNextHop().equalsIgnoreCase(vehId)) {
            return;
        }

        // Deliver directly to RSU if in range
        double distToRsu = computeRsuDistance(getOs().getPosition());
        if (distToRsu <= RSU_RANGE_M) {
            VehicleToRsuACK direct = new VehicleToRsuACK(
                newRouting(),
                ack.getOriginalMessageId(),
                ack.getTimestamp(),
                ack.getExpiryTimestamp(),
                ack.getVehicleIdentifier(),
                ack.getRsuDestination(),
                ack.getRsuDestination(),
                Collections.emptyList()
            );
            getOs().getAdHocModule().sendV2xMessage(direct);
            return;
        }

        // Forward to next hop
        List<String> checklist = new ArrayList<>(ack.getChecklist());
        if (checklist.isEmpty()) {
            return;
        }
        String nextHop = checklist.remove(0);
        logInfo("forwarding vehicle to rsu ack: uniqueId=" + ack.getId() + " nextHop=" + nextHop);
        VehicleToRsuACK fwd = new VehicleToRsuACK(
            newRouting(),
            ack.getOriginalMessageId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            ack.getVehicleIdentifier(),
            ack.getRsuDestination(),
            nextHop,
            checklist
        );
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }

    // Routing and path selection

    private MessageRouting newRouting() {
        // Create broadcast routing configuration
        return getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();
    }

    private String chooseNextHop(String dst) {
        // Find direct neighbors within transmission range
        List<String> direct = neighborGraph.entrySet().stream()
            .filter(e -> e.getValue().getDistanceFromVehicle() <= TX_RANGE_M)
            .map(Map.Entry::getKey)
            .filter(n -> !n.equalsIgnoreCase(vehId))
            .toList();

        if (direct.isEmpty()) {
            return null;
        }

        // Prefer two-hop neighbors that can reach the destination
        List<String> twoHop = direct.stream()
            .filter(n -> neighborGraph.get(n).getReachableNeighbors().contains(dst))
            .toList();
        if (!twoHop.isEmpty()) {
            return twoHop.stream()
                .min(Comparator.comparingDouble(n -> neighborGraph.get(n).getDistanceFromVehicle()))
                .get();
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
            .max(Comparator.comparingInt(n -> neighborGraph.get(n).getReachableNeighbors().size()))
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

            NodeRecord rec = neighborGraph.get(cur);
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

    // Utility methods

    private double computeRsuDistance(GeoPoint vehiclePosition) {
        // Compute distance to the closest RSU
        return STATIC_RSUS.values().stream()
            .mapToDouble(rsuPosition -> vehiclePosition.distanceTo(rsuPosition))
            .min()
            .orElse(Double.MAX_VALUE);
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
            .collect(Collectors.toList());
    }

    // Mandatory interface implementations
    @Override
    public void onMessageTransmitted(V2xMessageTransmission tx) {
        // No action required
    }

    @Override
    public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) {
        // No action required
    }

    @Override
    public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) {
        // No action required
    }

    // Logging
    private void logInfo(String msg) {
        getLog().infoSimTime(this, "[" + vehId + "] [info] " + msg);
    }
}