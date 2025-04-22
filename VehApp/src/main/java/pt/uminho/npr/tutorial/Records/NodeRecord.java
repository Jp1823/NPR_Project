package pt.uminho.npr.tutorial.Records;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class NodeRecord {

    private final double distanceFromVehicle;
    private final double distanceToClosestRsu;
    private final boolean reachableToRsu;
    private final List<String> reachableNeighbors;
    private final List<String> directNeighbors;
    private final long creationTimestamp;

    public NodeRecord(double distanceFromVehicle,
                      double distanceToClosestRsu,
                      boolean reachableToRsu,
                      List<String> reachableNeighbors,
                      List<String> directNeighbors,
                      long creationTimestamp) {
        this.distanceFromVehicle  = distanceFromVehicle;
        this.distanceToClosestRsu = distanceToClosestRsu;
        this.reachableToRsu       = reachableToRsu;
        this.reachableNeighbors   = Collections.unmodifiableList(
            List.copyOf(Objects.requireNonNull(reachableNeighbors))
        );
        this.directNeighbors      = Collections.unmodifiableList(
            List.copyOf(Objects.requireNonNull(directNeighbors))
        );
        this.creationTimestamp    = creationTimestamp;
    }

    public double getDistanceFromVehicle()  { return distanceFromVehicle; }
    public double getDistanceToClosestRsu() { return distanceToClosestRsu; }
    public boolean isReachableToRsu()       { return reachableToRsu; }
    public List<String> getReachableNeighbors() { return reachableNeighbors; }
    public List<String> getDirectNeighbors()    { return directNeighbors; }
    public long   getCreationTimestamp()    { return creationTimestamp; }

    @Override
    public String toString() {
        return "NODE_RECORD : DISTANCE_FROM_VEHICLE: " + distanceFromVehicle +
               " | DISTANCE_TO_CLOSEST_RSU: " + distanceToClosestRsu +
               " | REACHABLE_TO_RSU: " + reachableToRsu +
               " | REACHABLE_NEIGHBORS: " + reachableNeighbors.size() +
               " | DIRECT_NEIGHBORS: " + directNeighbors.size() +
               " | CREATION_TIMESTAMP: " + creationTimestamp;
    }
}