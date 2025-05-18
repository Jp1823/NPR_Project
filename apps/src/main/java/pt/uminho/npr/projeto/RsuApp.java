package pt.uminho.npr.projeto;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CellModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.objects.v2x.MessageRouting;
import org.eclipse.mosaic.lib.objects.v2x.V2xMessage;
import org.eclipse.mosaic.lib.util.scheduling.Event;
import org.eclipse.mosaic.rti.DATA;
import org.eclipse.mosaic.rti.TIME;

import pt.uminho.npr.projeto.messages.*;
import pt.uminho.npr.projeto.records.NodeRecord;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class RsuApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    private static final long   CLEAN_MS        = 1000 * TIME.MILLI_SECOND;
    private static final int    TX_POWER_DBM    = 23;
    private static final double TX_RANGE_M      = 120.0;
    private static final long   NEIGHBOR_TTL_MS = 1000 * TIME.MILLI_SECOND;

    private final Map<String, NodeRecord>  neighbors     = new HashMap<>();
    private final Map<String, Set<String>> seenIds       = new HashMap<>();
    private final Map<String,Integer>      fairnessCount = new HashMap<>();
    private final AtomicInteger            seqToVeh      = new AtomicInteger();
    private final AtomicInteger            seqToFog      = new AtomicInteger();
    private final AtomicInteger            seqAlert      = new AtomicInteger();
    private final Set<String>              seenAlerts    = new HashSet<>();
    private String fogId;
    
    private static final double W_TWO_HOP      = 50.0;
    private static final double W_PROXIMITY    = 30.0;
    private static final double W_CONNECTIVITY = 10.0;
    private static final double W_STABILITY    =  5.0;
    private static final double W_FAIRNESS     =  5.0;

    private static final Map<String,String> RSU_TO_FOG = new HashMap<>();
    static {
        RSU_TO_FOG.put("RSU_1", "server_0");
        RSU_TO_FOG.put("RSU_4", "server_0");
        RSU_TO_FOG.put("RSU_9", "server_0");

        RSU_TO_FOG.put("RSU_2", "server_1");
        RSU_TO_FOG.put("RSU_3", "server_1");
        RSU_TO_FOG.put("RSU_5", "server_1");

        RSU_TO_FOG.put("RSU_6", "server_2");
        RSU_TO_FOG.put("RSU_7", "server_2");

        RSU_TO_FOG.put("RSU_8",  "server_3");
        RSU_TO_FOG.put("RSU_10", "server_3");

        RSU_TO_FOG.put("RSU_11", "server_4");
        RSU_TO_FOG.put("RSU_12", "server_4");

        RSU_TO_FOG.put("RSU_13", "server_5");
        RSU_TO_FOG.put("RSU_0",  "server_5");
    }

    private static final List<String> ALL_RSUS = List.of(
        "rsu_0","rsu_1","rsu_2","rsu_3","rsu_4","rsu_5","rsu_6",
        "rsu_7","rsu_8","rsu_9","rsu_10","rsu_11","rsu_12","rsu_13"
    );

    @Override
    public void onStartup() {
        String rsuId = getOs().getId().toUpperCase(Locale.ROOT);
        logInfo(String.format("RSU_INITIALIZATION : RSU_ID: %s", rsuId));
        getOs().getAdHocModule().enable(
            new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(TX_POWER_DBM)
                .distance(TX_RANGE_M)
                .create()
        );
        getOs().getCellModule().enable(new CellModuleConfiguration()
            .maxDownlinkBitrate(50 * DATA.MEGABIT)
            .maxUplinkBitrate(50 * DATA.MEGABIT)
        );
        String myId = getOs().getId().toUpperCase(Locale.ROOT);
        fogId = RSU_TO_FOG.get(myId);
        scheduleCleanup();
    }

    @Override
    public void onShutdown() {
        printProcessedIds();
        getOs().getAdHocModule().disable();
        getOs().getCellModule().disable();
    }

    @Override
    public void processEvent(Event e) {
        purgeNeighbors();
        scheduleCleanup();
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage in) {
        V2xMessage m = in.getMessage();
        String type  = m.getClass().getSimpleName();
        String uid   = extractId(m);

        Set<String> seenForType = seenIds.computeIfAbsent(type, k -> new HashSet<>());
        if (!seenForType.add(uid)) {
            // logInfo("DUPLICATE MESSAGE ID = " + uid + " DETECTED – DISCARDED");
            return;
        }

        if (m instanceof VehicleToVehicle v2v) {
            // logInfo("PROCESSING VEHICLE TO VEHICLE MESSAGE ID = " + v2v.getMessageId());
            forwardToFog(v2v);
            handleV2V(v2v);

        } else if (m instanceof VehicleToRsuACK ack) {
            logInfo(String.format(
                "ACK_RECEIVED : UNIQUE_ID: %s | ORIGINAL_ID: %s", ack.getUniqueId(), ack.getOriginalMessageId()
            ));
            forwardToFog(ack);
            
        } else if (m instanceof FogToRsuMessage f2r) {
            logInfo(String.format(
                "FOG_COMMAND_RECEIVED : UNIQUE_ID: %s | EVENT_TYPE: %s", f2r.getUniqueId(), f2r.getCommandEvent().getEventType()
            ));
            handleFogCommand(f2r);
            emitRsuAlert(f2r.getCommandEvent());
        } else if (m instanceof RsuAlertMessage alert && seenAlerts.add(alert.getUniqueId())) {
            logInfo(String.format(
                "RSU_ALERT_RECEIVED : UNIQUE_ID: %s | ORIGIN: %s", alert.getUniqueId(), alert.getOriginRsu()
            ));
            handleLocalRsuAlert(alert);
        }
    }

    @Override public void onMessageTransmitted(V2xMessageTransmission tx) { }
    @Override public void onAcknowledgementReceived(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement ack) { }
    @Override public void onCamBuilding(org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder cb) { }

    private void forwardToFog(V2xMessage inner) {
        String rsuId = getOs().getId().toUpperCase(Locale.ROOT);
        MessageRouting routing = newRoutingCell();
        RsuToFogMessage rtf = new RsuToFogMessage(
            routing,
            String.format("RTF-%s-%s-%d", rsuId, fogId, seqToFog.getAndIncrement()),
            getOs().getSimulationTime(),
            getOs().getId().toUpperCase(Locale.ROOT),
            inner
        );
        getOs().getCellModule().sendV2xMessage(rtf);
        // logInfo("RTF MESSAGE ID = " + rtf.getUniqueId() + " SENT TO FOG ID = " + fogId);
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
    
        String dst = f2r.getVehicleTarget().toUpperCase(Locale.ROOT);
        NodeRecord rec = neighbors.get(dst);
    
        String nextHop = null;
        if (rec != null && rec.isReachableToRsu()) {
            nextHop = dst;
        } else {
            nextHop = selectNextHop(dst);
        }
    
        if (nextHop == null) {
            return;
        }
    
        List<String> initialTrail = List.of(rsuId);
    
        FogEventMessage event = f2r.getCommandEvent();
    
        RsuToVehicleMessage rtv = new RsuToVehicleMessage(
            newRoutingAdHoc(),
            String.format("RTV-%s-%s-%d", rsuId, dst, seqToVeh.getAndIncrement()),
            f2r.getTimestamp(),
            f2r.getExpiryTimestamp(),
            rsuId,
            dst,
            nextHop,
            event,
            initialTrail
        );
    
        getOs().getAdHocModule().sendV2xMessage(rtv);
        logInfo(String.format(
            "EVENT_FORWARDED_TO_VEHICLE : UNIQUE_ID: %s | VEHICLE_TARGET: %s | NEXT_HOP: %s",
            f2r.getCommandEvent().getUniqueId(), dst, nextHop
        ));
    }

    private void emitRsuAlert(FogEventMessage ev) {
        String origin = getOs().getId().toUpperCase(Locale.ROOT);
        for (String target : ALL_RSUS) {
            if (target.equalsIgnoreCase(origin)) continue;
            String alertId = String.format("ALERT-%s-%s-%d", origin, target.toUpperCase(), seqAlert.getAndIncrement());
            if (!seenAlerts.add(alertId)) continue;
            RsuAlertMessage alert = new RsuAlertMessage(
                newRoutingToRsu(target),
                alertId,
                getOs().getSimulationTime(),
                origin,
                ev
            );
            getOs().getCellModule().sendV2xMessage(alert);
            logInfo(String.format(
                "ALERT_EMITTED : UNIQUE_ID: %s | TARGET_RSU: %s", alertId, target.toUpperCase()
            ));
        }
    }

    private void handleLocalRsuAlert(RsuAlertMessage alert) {
        return;
    }

    private MessageRouting newRoutingAdHoc() {
        return getOs().getAdHocModule()
            .createMessageRouting()
            .broadcast()
            .topological()
            .build();
    }   

    private MessageRouting newRoutingCell() {
        return getOs().getCellModule()
            .createMessageRouting()
            .destination(fogId)
            .topological()
            .build();
    }

    private MessageRouting newRoutingToRsu(String rsuDest) {
        return getOs().getCellModule()
            .createMessageRouting()
            .destination(rsuDest)
            .topological()
            .build();
    }

    private String selectNextHop(String dst) {
        List<String> direct = neighbors.entrySet().stream()
            .filter(e -> e.getValue().isReachableToRsu())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        if (direct.isEmpty()) return null;
    
        Set<String> twoHop = direct.stream()
            .filter(n -> neighbors.get(n).getReachableNeighbors().contains(dst))
            .collect(Collectors.toSet());
    
        List<String> candidates = twoHop.isEmpty() ? direct : new ArrayList<>(twoHop);

        double maxDegree     = candidates.stream()
            .mapToInt(n -> neighbors.get(n).getReachableNeighbors().size())
            .max().orElse(1);
        int maxFairness      = fairnessCount.values().stream().mapToInt(i -> i).max().orElse(1);
        long now             = getOs().getSimulationTime();

        double maxStability  = candidates.stream()
            .mapToDouble(n -> now - neighbors.get(n).getCreationTimestamp())
            .max().orElse(1.0);
    
        String best     = null;
        double bestScore= Double.NEGATIVE_INFINITY;
        for (String cand : candidates) {
            NodeRecord rec = neighbors.get(cand);
    
            double scoreTwoHop = twoHop.contains(cand) ? 1.0 : 0.0;

            double scoreProx   = 1.0 / rec.getDistanceFromVehicle();

            double scoreConn   = rec.getReachableNeighbors().size() / maxDegree;

            double age         = now - rec.getCreationTimestamp();
            double scoreStab   = age / maxStability;

            int usedTimes      = fairnessCount.getOrDefault(cand, 0);
            double scoreFair   = 1.0 - (usedTimes / (double) maxFairness);
    
            double score = W_TWO_HOP      * scoreTwoHop
                         + W_PROXIMITY    * scoreProx
                         + W_CONNECTIVITY * scoreConn
                         + W_STABILITY    * scoreStab
                         + W_FAIRNESS     * scoreFair;
    
            if (score > bestScore) {
                bestScore = score;
                best      = cand;
            }
        }
    
        fairnessCount.merge(best, 1, Integer::sum);
        return best;
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

    /*
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
    */
    
    private void printProcessedIds() {
        /*
        Set<String> v2vIds = seenIds.getOrDefault("VehicleToVehicle", Collections.emptySet());
        logInfo("PROCESSED_V2V_IDS : COUNT = " + v2vIds.size());
        v2vIds.stream()
            .sorted((a, b) -> {
                String[] pa = a.split("-");
                String[] pb = b.split("-");
                int cmp = pa[1].compareTo(pb[1]);
                if (cmp != 0) return cmp;
                return Integer.compare(
                    Integer.parseInt(pa[2]),
                    Integer.parseInt(pb[2])
                );
            })
            .forEach(id -> logInfo("PROCESSED_V2V_ID : " + id));
        */
        Set<String> ackIds = seenIds.getOrDefault("VehicleToRsuACK", Collections.emptySet());
        logInfo("PROCESSED_ACK_IDS : COUNT = " + ackIds.size());
        ackIds.stream()
            .sorted((a, b) -> {
                String[] pa = a.split("-");
                String[] pb = b.split("-");
                int cmp = pa[1].compareTo(pb[1]);
                if (cmp != 0) return cmp;
                return Integer.compare(
                    Integer.parseInt(pa[2]),
                    Integer.parseInt(pb[2])
                );
            })
            .forEach(id -> logInfo("PROCESSED_ACK_ID : " + id));
    }    

    private void logInfo(String m) {
        getLog().infoSimTime(this, "[" + getOs().getId().toUpperCase(Locale.ROOT) + "] [INFO]  " + m);
    }
}