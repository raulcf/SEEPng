package uk.ac.imperial.lsds.seepcontrib.yarn.infrastructure;

import java.net.InetAddress;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnit;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.util.Utils;

public class YarnContainer implements ExecutionUnit {

    private static final ExecutionUnitType executionUnitType = ExecutionUnitType.YARN_CONTAINER;
    
    private EndPoint ep;
    private int id;
    
    public YarnContainer(InetAddress ip, int port, int dataPort) {
        this.id = Utils.computeIdFromIpAndPort(ip, port);
        this.ep = new EndPoint(id, ip, port, dataPort);
    }
    
    @Override
    public ExecutionUnitType getType() {
        return executionUnitType;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public EndPoint getEndPoint() {
        return ep;
    }

}
