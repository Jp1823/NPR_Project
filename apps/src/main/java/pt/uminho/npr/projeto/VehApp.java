package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
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

public final class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    private static final long   BEACON_PERIOD_MS   = 100 * TIME.MILLI_SECOND;
    private static final long   CLEAN_THRESHOLD_MS = 1000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM       = 23;
    private static final double TX_RANGE_M         = 150.0;
    private static final double RSU_RANGE_M        = 150.0;
    private static final int    INITIAL_TTL        = 10;
    private static final long   EVENT_DURATION_NS  = 30_000 * TIME.MILLI_SECOND;
    private static final long   ACCIDENT_DURATION_NS = 30_000 * TIME.MILLI_SECOND;
    private static final long   RESUME_ACCEL_DURATION_NS = 1_000 * TIME.MILLI_SECOND;

    private static final Map<String, GeoPoint> STATIC_RSUS = new HashMap<>();
    static {
        STATIC_RSUS.put("RSU_0",  GeoPoint.latLon(52.451033, 13.295327, 0));
        STATIC_RSUS.put("RSU_1",  GeoPoint.latLon(52.451406, 13.298062, 0));
        STATIC_RSUS.put("RSU_2",  GeoPoint.latLon(52.452119, 13.301154, 0));
        STATIC_RSUS.put("RSU_3",  GeoPoint.latLon(52.452153, 13.304256, 0));
        STATIC_RSUS.put("RSU_4",  GeoPoint.latLon(52.450304, 13.301368, 0));
        STATIC_RSUS.put("RSU_5",  GeoPoint.latLon(52.450268, 13.304328, 0));
        STATIC_RSUS.put("RSU_6",  GeoPoint.latLon(52.448599, 13.305743, 0));
        STATIC_RSUS.put("RSU_7",  GeoPoint.latLon(52.448538, 13.302883, 0));
        STATIC_RSUS.put("RSU_8",  GeoPoint.latLon(52.447641, 13.301310, 0));
        STATIC_RSUS.put("RSU_9",  GeoPoint.latLon(52.449290, 13.298481, 0));
        STATIC_RSUS.put("RSU_10", GeoPoint.latLon(52.446686, 13.298421, 0));
        STATIC_RSUS.put("RSU_11", GeoPoint.latLon(52.446557, 13.294261, 0));
        STATIC_RSUS.put("RSU_12", GeoPoint.latLon(52.448404, 13.295637, 0));
        STATIC_RSUS.put("RSU_13", GeoPoint.latLon(52.449206, 13.292616, 0));
    }
    
    private final Map<String, NodeRecord> neighborGraph = new HashMap<>();
    
    private final Set<String>   processedV2V = new HashSet<>();
    private final Set<String>   processedAck = new HashSet<>();
    private final AtomicInteger v2vSeq       = new AtomicInteger();
    private final AtomicInteger ackSeq       = new AtomicInteger();

    private String  vehId;
    private boolean ready = false;
    private double  heading = 0.0;
    private double  speed = 0.0;
    private double  preAccidentSpeed = 0.0;
    private boolean inAccident = false;

    private long lastPrintTime = 0;

    private volatile boolean pendingBrake = false;
    private volatile boolean pendingLaneChange = false;

    @Override
    public void onStartup() {

        // Initialize vehicle ID
        vehId = getOs().getId().toUpperCase(Locale.ROOT);

        // Enable ad-hoc communication module
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER_DBM)
                .distance(TX_RANGE_M)
                .create()
        );

        scheduleNext();
        logInfo("VEHICLE_INITIALIZATION");
    }

    @Override
    public void onShutdown() {
        printProcessedIds();
        getOs().getAdHocModule().disable();
        logInfo("VEHICLE_SHUTDOWN");
    }

    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                             @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData cur) {
        heading = cur.getHeading().doubleValue();
        speed   = cur.getSpeed();
        if (!ready) {
            ready = true;
        }
        if (pendingBrake && !inAccident) {
            inAccident = true;
            preAccidentSpeed = speed;
            logInfo("ACCIDENT EVENT RECEIVED: APPLYING EMERGENCY BRAKE FOR " 
                    + (ACCIDENT_DURATION_NS / TIME.MILLI_SECOND) + " ms");

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
            sendBeacon(now);
        }
        purgeStale(now);
        if (now - lastPrintTime >= 30000 * TIME.MILLI_SECOND) {
            // printNeighborGraph();
            // printProcessedIds();
            lastPrintTime = now;
        }
        scheduleNext();
    }

    private void scheduleNext() {
        getOs().getEventManager().addEvent(
            getOs().getSimulationTime() + BEACON_PERIOD_MS,
            this
        );
    }

    private void purgeStale(long now) {
        neighborGraph.entrySet()
                     .removeIf(e -> now - e.getValue().getCreationTimestamp() > CLEAN_THRESHOLD_MS);
    }

    private void sendBeacon(long now) {
        Map<String, NodeRecord> payload = new HashMap<>(neighborGraph);

        String msgId = "V2V-" + vehId + "-" + v2vSeq.getAndIncrement();
        MessageRouting routing = newRouting();
        VehicleToVehicle v2v = new VehicleToVehicle(
            routing,
            msgId,
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
        /*
        logInfo(String.format(
            "VEHICLE_TO_VEHICLE_MESSAGE : UNIQUE_ID = %s | SENDER = %s | TTL = %d | GRAPH_SIZE = %d",
            v2v.getMessageId(), v2v.getSenderId(), v2v.getTimeToLive(), payload.size()
        ));
        */
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage msg = in.getMessage();
        if (msg instanceof VehicleToVehicle v2v) {
            /*
            logInfo("RECEIVED VEHICLE_TO_VEHICLE : UNIQUE_ID = " + v2v.getMessageId() +
                    " | FROM = "      + v2v.getSenderId() +
                    " | TTL = "       + v2v.getTimeToLive() +
                    " | GRAPH_SIZE = "+ v2v.getNeighborGraph().size());
            */
            handleV2V(v2v);
        } else if (msg instanceof RsuToVehicleMessage rtv) {
            logInfo("RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                    " | RSU_SOURCE = "    + rtv.getRsuSource() +
                    " | VEHICLE_TARGET = "+ rtv.getVehicleTarget() +
                    " | NEXT_HOP = "      + rtv.getNextHop() +
                    " | COMMAND = "       + rtv.getCommandEvent());
            handleR2V(rtv);
        } else if (msg instanceof VehicleToRsuACK ack) {
            logInfo("VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + ack.getUniqueId() +
                    " | ORIGINAL_ID = "   + ack.getOriginalMessageId() +
                    " | NEXT_HOP = "      + ack.getNextHop());
            forwardAck(ack);
        }
    }

    private void handleV2V(VehicleToVehicle v2v) {
        if (!processedV2V.add(v2v.getMessageId())) return;

        String sender = v2v.getSenderId().toUpperCase(Locale.ROOT);
        GeoPoint senderPos = v2v.getPosition();

        double distSelf   = getOs().getPosition().distanceTo(senderPos);
        double distToRsu  = computeRsuDistance(senderPos);
        boolean reachableToRsu = distToRsu <= RSU_RANGE_M;

        List<String> reachableNeighbors = new ArrayList<>(v2v.getNeighborGraph().keySet());

        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = v2v.getNeighborGraph().get(id);
                return nr != null && nr.getDistanceFromVehicle() <= TX_RANGE_M;
            })
            .toList();

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

        if (v2v.getTimeToLive() > 1) {
            VehicleToVehicle fwd = new VehicleToVehicle(
                newRouting(),
                v2v.getMessageId(),
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

        if (rtv.getForwardingTrail().contains(vehId)) {
            return;
        }
        if (!rtv.getNextHop().equalsIgnoreCase(vehId)) {
            return;
        }

        if (rtv.getVehicleTarget().equalsIgnoreCase(vehId)) {
            logInfo("FINAL TARGET REACHED | SENDING ACK : UNIQUE_ID = " + rtv.getUniqueId());
            FogEventMessage ev = rtv.getCommandEvent();
            switch (ev.getEventType()) {
                case "ACCIDENT":
                    logInfo("ACCIDENT EVENT RECEIVED: SCHEDULING EMERGENCY BRAKE");
                    pendingBrake = true;
                    break;
                case "LANE_CLOSURE":
                    logInfo("LANE_CLOSURE EVENT RECEIVED: SCHEDULING LANE CHANGE");
                    pendingLaneChange = true;
                    break;
                default:
                    logInfo("UNKNOWN EVENT TYPE: " + ev.getEventType());
            }
            sendAck(rtv);
            return;
        }
    
        String dst = rtv.getVehicleTarget().toUpperCase(Locale.ROOT);
    
        NodeRecord rec = neighborGraph.get(dst);
        if (rec != null && rec.getDistanceFromVehicle() <= TX_RANGE_M) {
            // logInfo("DIRECT DELIVERY TO DESTINATION : " + dst);
            List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
            trail.add(vehId);
            
            RsuToVehicleMessage direct = new RsuToVehicleMessage(
                newRouting(),
                rtv.getUniqueId(),
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
    
        String next = chooseNextHop(dst);
        if (next == null) {
            // logDebug("NO ROUTE TO FORWARD RSU_TO_VEHICLE_MESSAGE : DEST = " + dst);
            return;
        }
        logInfo("FORWARDING RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + next);
        List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
        trail.add(vehId);
        RsuToVehicleMessage fwd = new RsuToVehicleMessage(
            newRouting(),
            rtv.getUniqueId(),
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

    private void sendAck(RsuToVehicleMessage rtv) {
        long now = getOs().getSimulationTime();
        String uid = "ACK-" + vehId + "-" + ackSeq.getAndIncrement();
    
        List<String> backPath = new ArrayList<>(rtv.getForwardingTrail());
        Collections.reverse(backPath);
    
        String nextHop = backPath.get(0);
    
        List<String> checklist = backPath.size() > 1
            ? new ArrayList<>(backPath.subList(1, backPath.size()))
            : Collections.emptyList();
        logInfo("SENDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + uid +
                " | ORIGINAL_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + nextHop);
        VehicleToRsuACK ack = new VehicleToRsuACK(
            newRouting(),
            uid,
            rtv.getUniqueId(),
            now,
            now + (10000 * TIME.MILLI_SECOND),
            vehId,
            rtv.getRsuSource(),
            nextHop,
            checklist
        );
        logInfo(ack.toString());
        processedAck.add(uid);
        getOs().getAdHocModule().sendV2xMessage(ack);
    }

    private void forwardAck(VehicleToRsuACK ack) {
    
        if (!processedAck.add(ack.getUniqueId())) {
            // logDebug("IGNORED VEHICLE_TO_RSU_ACK : DUPLICATE");
            return;
        }
    
        if (!ack.getNextHop().equalsIgnoreCase(vehId)) {
            // logDebug("IGNORED VEHICLE_TO_RSU_ACK : WRONG_NEXT_HOP");
            return;
        }
    
        double distToRsu = computeRsuDistance(getOs().getPosition());
        if (distToRsu <= RSU_RANGE_M) {
            // logInfo("DIRECT ACK DELIVERY TO RSU : UNIQUE_ID = " + ack.getUniqueId());
            VehicleToRsuACK direct = new VehicleToRsuACK(
                newRouting(),
                ack.getUniqueId(),
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
    
        List<String> checklist = new ArrayList<>(ack.getChecklist());
        String nextHop = checklist.remove(0);
        logInfo("FORWARDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + ack.getUniqueId()
              + " | NEXT_HOP = " + nextHop);
        VehicleToRsuACK fwd = new VehicleToRsuACK(
            newRouting(),
            ack.getUniqueId(),
            ack.getOriginalMessageId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            ack.getVehicleIdentifier(),
            ack.getRsuDestination(),
            nextHop,
            checklist
        );
        processedAck.add(ack.getUniqueId());
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }
 
    private MessageRouting newRouting() {
        return getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();
    }

    private String chooseNextHop(String dst) {
    
        List<String> direct = neighborGraph.entrySet().stream()
            .filter(e -> e.getValue().getDistanceFromVehicle() <= TX_RANGE_M)
            .map(Map.Entry::getKey)
            .filter(n -> !n.equalsIgnoreCase(vehId))
            .toList();
        if (direct.isEmpty()) return null;

        List<String> twoHop = direct.stream()
            .filter(n -> neighborGraph.get(n).getReachableNeighbors().contains(dst))
            .toList();
        if (!twoHop.isEmpty()) {
            return twoHop.stream()
                .min(Comparator.comparingDouble(n -> neighborGraph.get(n).getDistanceFromVehicle()))
                .get();
        }
    
        String bestByHops = direct.stream()
            .min(Comparator.comparingInt(n -> hopCountRestricted(n, dst, direct)))
            .orElse(null);
        if (bestByHops != null
            && hopCountRestricted(bestByHops, dst, direct) < Integer.MAX_VALUE) {
            return bestByHops;
        }

        return direct.stream()
            .max(Comparator.comparingInt(n -> neighborGraph.get(n).getReachableNeighbors().size()))
            .orElse(null);
    }
    
    private int hopCountRestricted(String start, String dst, List<String> directCandidates) {
        if (start.equalsIgnoreCase(dst)) return 0;
        Queue<String> queue = new LinkedList<>();
        Map<String,Integer> dist = new HashMap<>();
        queue.offer(start);
        dist.put(start, 0);
    
        Set<String> allowed = new HashSet<>(directCandidates);
        allowed.add(dst);
    
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            int hops = dist.get(cur);
            if (cur.equalsIgnoreCase(dst)) return hops;
    
            NodeRecord rec = neighborGraph.get(cur);
            if (rec == null) continue;
    
            for (String nb : rec.getReachableNeighbors()) {
                if (!allowed.contains(nb)) continue;
                if (dist.putIfAbsent(nb, hops + 1) == null) {
                    queue.offer(nb);
                }
            }
        }
        return Integer.MAX_VALUE;
    }
    
    /*
    private void printNeighborGraph() {
        logInfo("NEIGHBOR_GRAPH : SIZE = " + neighborGraph.size());
        neighborGraph.keySet().stream().sorted().forEach(key -> {
            NodeRecord rec = neighborGraph.get(key);
            logInfo("NEIGHBOR_ENTRY : VEHICLE_ID: " + key +
                    " | DISTANCE_FROM_VEHICLE: " + String.format("%.2f", rec.getDistanceFromVehicle()) +
                    " | DISTANCE_TO_CLOSEST_RSU: " + String.format("%.2f", rec.getDistanceToClosestRsu()) +
                    " | REACHABLE_TO_RSU: " + rec.isReachableToRsu() +
                    " | REACHABLE_NEIGHBORS: " + rec.getReachableNeighbors().size() +
                    " | DIRECT_NEIGHBORS: " + rec.getDirectNeighbors().size() +
                    " | CREATION_TIMESTAMP: " + rec.getCreationTimestamp());
        });
        logInfo("RSUS = " + staticRsus.size());
        staticRsus.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
            String rsuId = e.getKey();
            GeoPoint p   = e.getValue();
            double dist  = distance(getOs().getPosition(), p);
            boolean reachable = dist <= RSU_RANGE_M;
            logInfo("RSU_ENTRY : RSU_ID: " + rsuId +
                    " | DISTANCE: "         + String.format("%.2f", dist) +
                    " | REACHABLE: "        + reachable);
        });
    }
    */

    private void printProcessedIds() {
        /*
        logInfo("PROCESSED_V2V_IDS : COUNT = " + processedV2V.size());
        processedV2V.stream().sorted((a, b) -> {
            String[] pa = a.split("-");
            String[] pb = b.split("-");
            int cmp = pa[1].compareTo(pb[1]);
            if (cmp != 0) return cmp;
            return Integer.compare(Integer.parseInt(pa[2]), Integer.parseInt(pb[2]));
        }).forEach(id -> logInfo("PROCESSED_V2V_ID : " + id));
        */
        logInfo("PROCESSED_ACK_IDS : COUNT = " + processedAck.size());
        processedAck.stream().sorted((a, b) -> {
            String[] pa = a.split("-");
            String[] pb = b.split("-");
            int cmp = pa[1].compareTo(pb[1]);
            if (cmp != 0) return cmp;
            return Integer.compare(Integer.parseInt(pa[2]), Integer.parseInt(pb[2]));
        }).forEach(id -> logInfo("PROCESSED_ACK_ID : " + id));
    }

    private double computeRsuDistance(GeoPoint p) {
        return p.distanceTo(STATIC_RSUS.get("RSU_0"));
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { }
    @Override public void onAcknowledgementReceived(ReceivedAcknowledgement ack) { }
    @Override public void onCamBuilding(CamBuilder cb) { }

    private void logInfo(String msg) { 
        getLog().infoSimTime(this, "[" + vehId + "] [INFO]  " + msg);
    }
}