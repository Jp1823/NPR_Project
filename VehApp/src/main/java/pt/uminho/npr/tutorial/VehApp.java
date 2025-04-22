package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.*;
import pt.uminho.npr.tutorial.Records.NodeRecord;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.VehicleApplication;
import org.eclipse.mosaic.fed.application.app.api.os.VehicleOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.geo.GeoPoint;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;

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
    private static final double RSU_RANGE_M        = 100.0;
    private static final int    INITIAL_TTL        = 10;

    private final Map<String, GeoPoint>   staticRsus    = new HashMap<>();
    private final Map<String, NodeRecord> neighborGraph = new HashMap<>();

    private final Set<String> processedV2V   = new HashSet<>();
    private final Set<String> processedAck   = new HashSet<>();

    private final AtomicInteger v2vSeq       = new AtomicInteger();
    private final AtomicInteger ackSeq       = new AtomicInteger();

    private boolean ready = false;
    private double  heading, speed;
    // private long lastPrintTime = 0;

    private void logInfo(String msg)  { getLog().infoSimTime(this, "[" + id() + "] [INFO]  " + msg); }
    private void logDebug(String msg) { getLog().debugSimTime(this, "[" + id() + "] [DEBUG] " + msg); }
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
        printNeighborGraph();
        printProcessedIds();
        getOs().getAdHocModule().disable();
    }

    @Override
    public void onVehicleUpdated(@Nullable org.eclipse.mosaic.lib.objects.vehicle.VehicleData prev,
                                 @Nonnull org.eclipse.mosaic.lib.objects.vehicle.VehicleData cur) {
        heading = cur.getHeading().doubleValue();
        speed   = cur.getSpeed();
        if (!ready) {
            ready = true;
        }
        printNeighborGraph();
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
            logInfo("RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                    " | RSU_SOURCE = "    + rtv.getRsuSource() +
                    " | VEHICLE_TARGET = "+ rtv.getVehicleTarget() +
                    " | NEXT_HOP = "      + rtv.getNextHop() +
                    " | COMMAND = "       + rtv.getCommand());
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
        boolean reachable = distSelf <= RSU_RANGE_M;
        double distToRsu  = computeRsuDistance(senderPos);

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
            reachable,
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
            /*
            logDebug("IGNORED RSU_TO_VEHICLE_MESSAGE : LOOP_DETECTED"); 
            */
            return;
        }
        if (!rtv.getNextHop().equalsIgnoreCase(me)) {
            /*
            logDebug("IGNORED RSU_TO_VEHICLE_MESSAGE : WRONG_NEXT_HOP"); 
            */
            return;
        }
        if (rtv.getVehicleTarget().equalsIgnoreCase(me)) {
            logInfo("FINAL TARGET REACHED | SENDING ACK : UNIQUE_ID = " + rtv.getUniqueId());
            sendAck(rtv); return;
        }
        String dst  = rtv.getVehicleTarget().toUpperCase(Locale.ROOT);
        String next = chooseNextHop(dst);
        if (next == null) {
            logDebug("NO ROUTE TO FORWARD RSU_TO_VEHICLE_MESSAGE : DEST = " + dst); 
            return;
        }
        logInfo("FORWARDING RSU_TO_VEHICLE_MESSAGE : UNIQUE_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + next);
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
            rtv.getCommand(),
            trail
        );
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }

    private void sendAck(RsuToVehicleMessage rtv) {
        long now    = getOs().getSimulationTime();
        String uid = "ACK-" + id() + "-" + ackSeq.getAndIncrement();
        List<String> path = new ArrayList<>(rtv.getForwardingTrail());
        path.add(id());
        Collections.reverse(path);
        String next = path.remove(0);
        logInfo("SENDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + uid +
                " | ORIGINAL_ID = " + rtv.getUniqueId() +
                " | NEXT_HOP = " + next);
        VehicleToRsuACK ack = new VehicleToRsuACK(
            newRouting(),
            uid,
            rtv.getUniqueId(),
            now,
            now + 10_000,
            id(),
            rtv.getRsuSource(),
            next,
            path
        );
        processedAck.add(uid);
        getOs().getAdHocModule().sendV2xMessage(ack);
    }

    private void forwardAck(VehicleToRsuACK ack) {
        if (!ack.getNextHop().equalsIgnoreCase(id())) {
            logDebug("IGNORED VEHICLE_TO_RSU_ACK : WRONG_NEXT_HOP"); return;
        }
        if (!processedAck.add(ack.getUniqueId())) {
            logDebug("IGNORED VEHICLE_TO_RSU_ACK : DUPLICATE"); return;
        }
        List<String> checklist = new ArrayList<>(ack.getChecklist());
        checklist.remove(0);
        String next = checklist.isEmpty() ? ack.getRsuDestination() : checklist.get(0);
        logInfo("FORWARDING VEHICLE_TO_RSU_ACK : UNIQUE_ID = " + ack.getUniqueId() +
                " | NEXT_HOP = " + next);
        VehicleToRsuACK fwd = new VehicleToRsuACK(
            newRouting(),
            ack.getUniqueId(),
            ack.getOriginalMessageId(),
            ack.getTimestamp(),
            ack.getExpiryTimestamp(),
            ack.getVehicleIdentifier(),
            ack.getRsuDestination(),
            next,
            checklist
        );
        getOs().getAdHocModule().sendV2xMessage(fwd);
    }

    private MessageRouting newRouting() {
        return getOs().getAdHocModule()
                      .createMessageRouting()
                      .viaChannel(AdHocChannel.CCH)
                      .topoBroadCast();
    }

    private String chooseNextHop(String dst) {
        return neighborGraph.keySet().stream()
            .min(Comparator.comparingInt(a -> hopCount(a, dst)))
            .orElse(null);
    }

    private int hopCount(String start, String dst) {
        if (start.equalsIgnoreCase(dst)) return 0;
        Queue<String> queue = new LinkedList<>();
        Map<String, Integer> distMap = new HashMap<>();
        queue.offer(start);
        distMap.put(start, 0);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            int hops = distMap.get(current);
            if (current.equalsIgnoreCase(dst)) return hops;

            NodeRecord rec = neighborGraph.get(current);
            if (rec != null) {
                for (String neighbor : rec.getNeighbourIdentifiers()) {
                    if (distMap.putIfAbsent(neighbor, hops + 1) == null) {
                        queue.offer(neighbor);
                    }
                }
            }
        }
        return Integer.MAX_VALUE;
    }

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