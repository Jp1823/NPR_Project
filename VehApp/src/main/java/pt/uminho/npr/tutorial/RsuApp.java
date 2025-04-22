package pt.uminho.npr.tutorial;

import pt.uminho.npr.tutorial.Messages.*;
import pt.uminho.npr.tutorial.Records.NodeRecord;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.TIME;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private static final long   CLEAN_MS        = 1000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM    = 23;
    private static final double TX_RANGE_M      = 200.0;
    private static final long   NEIGHBOR_TTL_MS = 1000 * TIME.MILLI_SECOND;

    private final Map<String, NodeRecord>  neighbors = new HashMap<>();
    private final Map<String, Set<String>> seenIds   = new HashMap<>();
    private final AtomicInteger            seqToVeh  = new AtomicInteger();
    private final AtomicInteger            seqToFog  = new AtomicInteger();

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
        scheduleCleanup();
    }

    @Override
    public void onShutdown() {
        printNeighborEntries();
        printSeenIds();
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event e) {
        purgeNeighbors();
        printNeighborEntries();
        scheduleCleanup();
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage m   = in.getMessage();
        String type    = m.getClass().getSimpleName();
        String uid     = extractId(m);

        Set<String> seenForType = seenIds.computeIfAbsent(type, k -> new HashSet<>());
        if (!seenForType.add(uid)) {
            return;
        }

        if (m instanceof VehicleToVehicle v2v) {
            forwardToFog(v2v);
            handleV2V(v2v);

        } else if (m instanceof VehicleToRsuACK ack) {
            forwardToFog(ack);

        } else if (m instanceof FogToRsuMessage f2r) {
            handleFogCommand(f2r);
        }
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { }

    private void forwardToFog(V2xMessage inner) {
        MessageRouting routing = newRouting();
        RsuToFogMessage rtf = new RsuToFogMessage(
            routing,
            "RTF-" + seqToFog.getAndIncrement(),
            getOs().getSimulationTime(),
            getOs().getId().toUpperCase(Locale.ROOT),
            "RSU_2",
            inner
        );
        getOs().getAdHocModule().sendV2xMessage(rtf);
    }

    private void handleV2V(VehicleToVehicle v2v) {
        String vid = v2v.getSenderId().toUpperCase(Locale.ROOT);
        double d   = distance(getOs().getPosition(), v2v.getPosition());
        boolean reachable = d <= TX_RANGE_M;
        List<String> reachableNeighbors = new ArrayList<>(v2v.getNeighborGraph().keySet());
        List<String> directNeighbors = reachableNeighbors.stream()
            .filter(id -> {
                NodeRecord nr = v2v.getNeighborGraph().get(id);
                return nr != null && nr.getDistanceFromVehicle() <= TX_RANGE_M;
            })
            .collect(Collectors.toList());

        NodeRecord rec = new NodeRecord(
            d,
            d,
            reachable,
            reachableNeighbors,
            directNeighbors,
            v2v.getTimeStamp()
        );
        neighbors.put(vid, rec);
    }

    private void handleFogCommand(FogToRsuMessage f2r) {
        String rsuId = getOs().getId().toUpperCase(Locale.ROOT);
        if (!(f2r.getRsuTarget().equalsIgnoreCase(rsuId) || f2r.getRsuTarget().equalsIgnoreCase("ALL"))) {
            return;
        }
        String dst  = f2r.getVehicleTarget().toUpperCase(Locale.ROOT);
        String next = neighbors.containsKey(dst) ? dst : selectNextHop(dst);
        if (next == null) {
            return;
        }
        MessageRouting routing = newRouting();
        List<String> initialTrail = List.of(getOs().getId().toUpperCase(Locale.ROOT));  
        RsuToVehicleMessage rtv = new RsuToVehicleMessage(
            routing,
            "RTV-" + seqToVeh.getAndIncrement(),
            f2r.getTimestamp(),
            f2r.getExpiryTimestamp(),
            getOs().getId().toUpperCase(Locale.ROOT),
            dst,
            next,
            f2r.getCommand(),
            initialTrail
        );
        logInfo("SENT RSU-TO-VEHICLE MESSAGE ID = " + rtv.getUniqueId()
                + " | NEXT_HOP = " + next
                + " | COMMAND = " + f2r.getCommand());
        getOs().getAdHocModule().sendV2xMessage(rtv);
    }

    private MessageRouting newRouting() {
        return getOs().getAdHocModule()
                      .createMessageRouting()
                      .viaChannel(AdHocChannel.CCH)
                      .topoBroadCast();
    }

    private String selectNextHop(String dst) {
        String best = null;
        int bestScore = Integer.MIN_VALUE;
        for (Map.Entry<String, NodeRecord> e : neighbors.entrySet()) {
            String cand = e.getKey();
            int hops = hopCount(cand, dst);
            if (hops == Integer.MAX_VALUE) continue;
            int score = e.getValue().getNeighbourIdentifiers().size() - hops;
            if (score > bestScore) {
                best = cand;
                bestScore = score;
            }
        }
        return best;
    }

    private int hopCount(String start, String dst) {
        if (start.equalsIgnoreCase(dst)) return 0;
        Queue<String> q = new LinkedList<>();
        Map<String,Integer> d = new HashMap<>();
        q.offer(start); d.put(start, 0);
        while (!q.isEmpty()) {
            String cur = q.poll();
            int hops = d.get(cur);
            if (cur.equalsIgnoreCase(dst)) return hops;
            NodeRecord rec = neighbors.get(cur);
            if (rec != null) {
                for (String n : rec.getNeighbourIdentifiers()) {
                    if (d.putIfAbsent(n, hops + 1) == null) {
                        q.offer(n);
                    }
                }
            }
        }
        return Integer.MAX_VALUE;
    }

    private void purgeNeighbors() {
        long now = getOs().getSimulationTime();
        neighbors.entrySet().removeIf(e ->
            (now - e.getValue().getCreationTimestamp() > NEIGHBOR_TTL_MS)
            || !e.getValue().isReachableToRsu()
        );
    }

    private String extractId(V2xMessage m) {
        if (m instanceof VehicleToVehicle v2v)      return v2v.getMessageId();
        if (m instanceof VehicleToRsuACK ack)       return ack.getUniqueId();
        if (m instanceof FogToRsuMessage f2r)       return f2r.getUniqueId();
        if (m instanceof RsuToVehicleMessage rtv)   return rtv.getUniqueId();
        if (m instanceof RsuToFogMessage rtf)       return rtf.getUniqueId();
        return UUID.randomUUID().toString();
    }

    private double distance(org.eclipse.mosaic.lib.geo.GeoPoint a, org.eclipse.mosaic.lib.geo.GeoPoint b) {
        double R = 6_371_000;
        double dLat = Math.toRadians(b.getLatitude() - a.getLatitude());
        double dLon = Math.toRadians(b.getLongitude() - a.getLongitude());
        double sLat = Math.sin(dLat / 2);
        double sLon = Math.sin(dLon / 2);
        double h = sLat * sLat +
                   Math.cos(Math.toRadians(a.getLatitude())) *
                   Math.cos(Math.toRadians(b.getLatitude())) *
                   sLon * sLon;
        return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
    }

    private void scheduleCleanup() {
        long next = getOs().getSimulationTime() + CLEAN_MS;
        getOs().getEventManager().addEvent(next, this);
    }

    private void printNeighborEntries() {
        logInfo("NEIGHBOR_GRAPH : SIZE = " + neighbors.size());
        neighbors.keySet().stream().sorted().forEach(key -> {
            NodeRecord rec = neighbors.get(key);
            logInfo("NEIGHBOR_ENTRY : VEHICLE_ID: " + key +
                    " | DISTANCE_FROM_RSU: " + String.format("%.2f", rec.getDistanceFromVehicle()) +
                    " | REACHABLE_TO_RSU: " + rec.isReachableToRsu() +
                    " | REACHABLE_NEIGHBORS: " + rec.getReachableNeighbors().size() +
                    " | DIRECT_NEIGHBORS: " + rec.getDirectNeighbors().size() +
                    " | CREATION_TIMESTAMP: " + rec.getCreationTimestamp());
        });
    }

    private void printSeenIds() {
        logInfo("SEEN_IDS_SUMMARY : TYPES = " + seenIds.keySet().size());
        seenIds.keySet().stream().sorted().forEach(type -> {
            Set<String> ids = seenIds.get(type);
            logInfo("SEEN_IDS_FOR_TYPE : TYPE = " + type + " | COUNT = " + ids.size());
            ids.stream().sorted().forEach(id ->
                logInfo("SEEN_ID : TYPE = " + type + " | UNIQUE_ID = " + id)
            );
        });
    }

    private void logInfo(String m) {
        getLog().infoSimTime(this, "[" + getOs().getId().toUpperCase(Locale.ROOT) + "] [INFO]  " + m);
    }
}