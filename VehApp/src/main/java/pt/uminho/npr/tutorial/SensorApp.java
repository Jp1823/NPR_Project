package pt.uminho.npr.tutorial;

import org.eclipse.mosaic.fed.application.app.AbstractApplication;
import org.eclipse.mosaic.fed.application.app.api.os.RoadSideUnitOperatingSystem;
import org.eclipse.mosaic.fed.application.app.api.CommunicationApplication;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.AdHocModuleConfiguration;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedV2xMessage;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.ReceivedAcknowledgement;
import org.eclipse.mosaic.fed.application.ambassador.simulation.communication.CamBuilder;
import org.eclipse.mosaic.interactions.communication.V2xMessageTransmission;
import org.eclipse.mosaic.lib.enums.AdHocChannel;
import org.eclipse.mosaic.lib.util.scheduling.Event;

public class SensorApp extends AbstractApplication<RoadSideUnitOperatingSystem>
        implements CommunicationApplication {

    @Override
    public void onStartup() {
        getOs().getAdHocModule().enable(new AdHocModuleConfiguration()
                .addRadio()
                .channel(AdHocChannel.CCH)
                .power(30)
                .distance(80.0)
                .create());
        getLog().infoSimTime(this, "SENSORAPP ON STARTUP: SENSOR ACTIVE");
    }

    @Override
    public void onShutdown() {
        getLog().infoSimTime(this, "SENSORAPP ON SHUTDOWN: SENSOR TERMINATED");
        getOs().getAdHocModule().disable();
    }

    @Override
    public void processEvent(Event event) throws Exception {
        getOs().getEventManager().addEvent(getOs().getSimulationTime() + 5000, this);
    }

    @Override
    public void onMessageReceived(ReceivedV2xMessage message) {
        getLog().infoSimTime(this, "SENSORAPP RECEIVED MESSAGE: " + message.getMessage().toString());
    }

    @Override
    public void onMessageTransmitted(V2xMessageTransmission transmission) {
        getLog().infoSimTime(this, "SENSORAPP ON MESSAGE TRANSMITTED");
    }

    @Override
    public void onAcknowledgementReceived(ReceivedAcknowledgement ack) {
        getLog().infoSimTime(this, "SENSORAPP RECEIVED ACKNOWLEDGEMENT: " + ack.toString());
    }

    @Override
    public void onCamBuilding(CamBuilder camBuilder) {
        getLog().infoSimTime(this, "SENSORAPP ON CAM BUILDING");
    }
}
