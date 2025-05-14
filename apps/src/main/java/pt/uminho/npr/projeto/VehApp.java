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
import java.util.stream.Collectors;

public final class VehApp extends AbstractApplication<VehicleOperatingSystem>
        implements VehicleApplication, CommunicationApplication {

    private static final long   BEACON_PERIOD_MS   = 100 * TIME.MILLI_SECOND;
    private static final long   CLEAN_THRESHOLD_MS = 1000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM       = 23;
    private static final double TX_RANGE_M         = 150.0;
    private static final double RSU_RANGE_M        = 150.0;
    private static final int    INITIAL_TTL        = 10;
    private static final long   EVENT_DURATION_NS  = 5_000 * TIME.MILLI_SECOND;
    private static final long   ACCIDENT_DURATION_NS = 5_000 * TIME.MILLI_SECOND;
    private static final long   RESUME_ACCEL_DURATION_NS = 1_000 * TIME.MILLI_SECOND;

    private final Map<String, GeoPoint>   staticRsus    = new HashMap<>();
    private final Map<String, NodeRecord> neighborGraph = new HashMap<>();

    private final Set<String> processedV2V   = new HashSet<>();
    private final Set<String> processedAck   = new HashSet<>();

    private final AtomicInteger v2vSeq       = new AtomicInteger();
    private final AtomicInteger ackSeq       = new AtomicInteger();

    private boolean ready = false;
    private double  heading, speed;
    private double  preAccidentSpeed = 0.0;
    private boolean inAccident = false;

    // private long lastPrintTime = 0;

    private volatile boolean pendingBrake = false;
    private volatile boolean pendingLaneChange = false;

    private void logInfo(String msg)  { getLog().infoSimTime(this, "[" + id() + "] [INFO]  " + msg); }
    // private void logDebug(String msg) { getLog().debugSimTime(this, "[" + id() + "] [DEBUG] " + msg); }
    private String id()               { return getOs().getId().toUpperCase(Locale.ROOT); }

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
        staticRsus.put("RSU_0", GeoPoint.latLon(52.454223, 13.313477, 0));
        logInfo("VEHICLE APP INITIALIZED");
        scheduleNext();
    }

    @Override
    public void onShutdown() {
        logInfo("VEHICLE APP TERMINATED");
        printProcessedIds();
        getOs().getAdHocModule().disable();
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
        /*
        if (now - lastPrintTime >= 2000 * TIME.MILLI_SECOND) {
            printNeighborGraph();
            printProcessedIds();
            lastPrintTime = now;
        }
        */
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

        String msgId = "V2V-" + id() + "-" + v2vSeq.getAndIncrement();
        MessageRouting routing = newRouting();
        VehicleToVehicle v2v = new VehicleToVehicle(
            routing,
            msgId,
            now,
            id(),
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
            /*
            logInfo("RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                    " | RSU_SOURCE = "    + rtv.getRsuSource() +
                    " | VEHICLE_TARGET = "+ rtv.getVehicleTarget() +
                    " | NEXT_HOP = "      + rtv.getNextHop() +
                    " | COMMAND = "       + rtv.getCommand());
            */
            handleR2V(rtv);
        } else if (msg instanceof VehicleToRsuACK ack) {
            /*
            logInfo("VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + ack.getUniqueId() +
                    " | ORIGINAL_ID = "   + ack.getOriginalMessageId() +
                    " | NEXT_HOP = "      + ack.getNextHop());
            */
            forwardAck(ack);
        }
    }

    private void handleV2V(VehicleToVehicle v2v) {
        if (!processedV2V.add(v2v.getMessageId())) return;

        String sender = v2v.getSenderId().toUpperCase(Locale.ROOT);
        GeoPoint senderPos = v2v.getPosition();

        double distSelf   = distance(getOs().getPosition(), senderPos);
        double distToRsu  = computeRsuDistance(senderPos);
        boolean reachableToRsu = distToRsu <= RSU_RANGE_M;

        List<String> reachableNeighbors = new ArrayList<>(v2v.getNeighborGraph().keySet());

        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = v2v.getNeighborGraph().get(id);
                return nr != null && nr.getDistanceFromVehicle() <= TX_RANGE_M;
            })
            .collect(Collectors.toList());

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

        String me = id();
        if (rtv.getForwardingTrail().contains(me)) {
            return;
        }
        if (!rtv.getNextHop().equalsIgnoreCase(me)) {
            return;
        }

        if (rtv.getVehicleTarget().equalsIgnoreCase(me)) {
            // logInfo("FINAL TARGET REACHED | SENDING ACK : UNIQUE_ID = " + rtv.getUniqueId());
            FogEventMessage ev = rtv.getCommandEvent();
            switch (ev.getEventType()) {
                case "ACCIDENT":
                    // logInfo("ACCIDENT EVENT RECEIVED: SCHEDULING EMERGENCY BRAKE");
                    pendingBrake = true;
                    break;
                case "LANE_CLOSURE":
                    // logInfo("LANE_CLOSURE EVENT RECEIVED: SCHEDULING LANE CHANGE");
                    pendingLaneChange = true;
                    break;
                default:
                    // logInfo("UNKNOWN EVENT TYPE: " + ev.getEventType());
            }
            sendAck(rtv);
            return;
        }
    
        String dst = rtv.getVehicleTarget().toUpperCase(Locale.ROOT);
    
        NodeRecord rec = neighborGraph.get(dst);
        if (rec != null && rec.getDistanceFromVehicle() <= TX_RANGE_M) {
            // logInfo("DIRECT DELIVERY TO DESTINATION : " + dst);
            List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
            trail.add(me);
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
        /*
        logInfo("FORWARDING RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + next);
        */
        List<String> trail = new ArrayList<>(rtv.getForwardingTrail());
        trail.add(me);
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
        String uid = "ACK-" + id() + "-" + ackSeq.getAndIncrement();
    
        List<String> backPath = new ArrayList<>(rtv.getForwardingTrail());
        Collections.reverse(backPath);
    
        String nextHop = backPath.get(0);
    
        List<String> checklist = backPath.size() > 1
            ? new ArrayList<>(backPath.subList(1, backPath.size()))
            : Collections.emptyList();
        /*
        logInfo("SENDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + uid +
                " | ORIGINAL_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + nextHop);
        */
        VehicleToRsuACK ack = new VehicleToRsuACK(
            newRouting(),
            uid,
            rtv.getUniqueId(),
            now,
            now + (10000 * TIME.MILLI_SECOND),
            id(),
            rtv.getRsuSource(),
            nextHop,
            checklist
        );
        logInfo(ack.toString());
        processedAck.add(uid);
        getOs().getAdHocModule().sendV2xMessage(ack);
    }

    private void forwardAck(VehicleToRsuACK ack) {
        String me = id();
    
        if (!processedAck.add(ack.getUniqueId())) {
            // logDebug("IGNORED VEHICLE_TO_RSU_ACK : DUPLICATE");
            return;
        }
    
        if (!ack.getNextHop().equalsIgnoreCase(me)) {
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
        /*
        logInfo("FORWARDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + ack.getUniqueId()
              + " | NEXT_HOP = " + nextHop);
        */
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
        String me = id();
    
        List<String> direct = neighborGraph.entrySet().stream()
            .filter(e -> e.getValue().getDistanceFromVehicle() <= TX_RANGE_M)
            .map(Map.Entry::getKey)
            .filter(n -> !n.equalsIgnoreCase(me))
            .collect(Collectors.toList());
        if (direct.isEmpty()) return null;

        List<String> twoHop = direct.stream()
            .filter(n -> neighborGraph.get(n).getReachableNeighbors().contains(dst))
            .collect(Collectors.toList());
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
    }
    */

    private void printProcessedIds() {
        logInfo("PROCESSED_V2V_IDS : COUNT = " + processedV2V.size());
        processedV2V.stream().sorted((a, b) -> {
            String[] pa = a.split("-");
            String[] pb = b.split("-");
            int cmp = pa[1].compareTo(pb[1]);
            if (cmp != 0) return cmp;
            return Integer.compare(Integer.parseInt(pa[2]), Integer.parseInt(pb[2]));
        }).forEach(id -> logInfo("PROCESSED_V2V_ID : " + id));
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
        return distance(p, staticRsus.get("RSU_0"));
    }

    private double distance(GeoPoint a, GeoPoint b) {
        double R = 6_371_000;
        double dLat = Math.toRadians(b.getLatitude() - a.getLatitude());
        double dLon = Math.toRadians(b.getLongitude() - a.getLongitude());
        double sLat = Math.sin(dLat / 2);
        double sLon = Math.sin(dLon / 2);
        double h    = sLat * sLat +
                      Math.cos(Math.toRadians(a.getLatitude())) *
                      Math.cos(Math.toRadians(b.getLatitude())) *
                      sLon * sLon;
        return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { }
    @Override public void onAcknowledgementReceived(ReceivedAcknowledgement ack) { }
    @Override public void onCamBuilding(CamBuilder cb) { }
}