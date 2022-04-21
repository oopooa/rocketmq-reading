package org.apache.rocketmq.namesrv.controller.manager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterInSyncReplicasResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ErrorCodes;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.controller.manager.event.AlterSyncStateSetEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.ApplyBrokerIdEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.ControllerResult;
import org.apache.rocketmq.namesrv.controller.manager.event.ElectMasterEvent;
import org.apache.rocketmq.namesrv.controller.manager.event.EventMessage;
import org.apache.rocketmq.namesrv.controller.manager.event.EventType;

/**
 * The manager that manages the replicas info for all brokers.
 * We can think of this class as the controller's memory state machine
 * It should be noted that this class is not thread safe,
 * and the upper layer needs to ensure that it can be called sequentially
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:00
 */
public class ReplicasInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Long LEADER_ID = 0L;
    private final boolean enableElectUncleanMaster;
    private final Map<String/* brokerName */, BrokerIdInfo> replicaInfoTable;
    private final Map<String/* brokerName */, InSyncReplicasInfo> inSyncReplicasInfoTable;

    public ReplicasInfoManager(final boolean enableElectUncleanMaster) {
        this.enableElectUncleanMaster = enableElectUncleanMaster;
        this.replicaInfoTable = new HashMap<>();
        this.inSyncReplicasInfoTable = new HashMap<>();
    }

    /********************************    The following methods don't update statemachine, triggered by controller leader   ********************************/

    public ControllerResult<AlterInSyncReplicasResponseHeader> alterSyncStateSet(
        final AlterInSyncReplicasRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<AlterInSyncReplicasResponseHeader> result = new ControllerResult<>(new AlterInSyncReplicasResponseHeader());
        final AlterInSyncReplicasResponseHeader response = result.getResponse();

        if (isContainsBroker(brokerName)) {
            final Set<String> newSyncStateSet = request.getNewSyncStateSet();
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();

            // Check master
            if (!replicasInfo.getMasterAddress().equals(request.getMasterAddress())) {
                log.info("Rejecting alter syncStateSet request because the current leader is:{}, not {}",
                    replicasInfo.getMasterAddress(), request.getMasterAddress());
                response.setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
                return result;
            }

            // Check master epoch
            if (request.getMasterEpoch() != replicasInfo.getMasterEpoch()) {
                log.info("Rejecting alter syncStateSet request because the current master epoch is:{}, not {}",
                    replicasInfo.getMasterEpoch(), request.getMasterEpoch());
                response.setErrorCode(ErrorCodes.FENCED_LEADER_EPOCH.getCode());
                return result;
            }

            // Check syncStateSet epoch
            if (request.getSyncStateSetEpoch() != replicasInfo.getSyncStateSetEpoch()) {
                log.info("Rejecting alter syncStateSet request because the current syncStateSet epoch is:{}, not {}",
                    replicasInfo.getSyncStateSetEpoch(), request.getSyncStateSetEpoch());
                response.setErrorCode(ErrorCodes.FENCED_SYNC_STATE_SET_EPOCH.getCode());
                return result;
            }

            // Check newSyncStateSet correctness
            for (String replicas : newSyncStateSet) {
                if (!brokerIdTable.containsKey(replicas)) {
                    log.info("Rejecting alter syncStateSet request because the replicas {} don't exist", replicas);
                    response.setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
                    return result;
                }
                // todo: check whether the replicas is active
            }
            if (!newSyncStateSet.contains(replicasInfo.getMasterAddress())) {
                log.info("Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {}", replicasInfo.getMasterAddress());
                response.setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
                return result;
            }

            // Generate event
            response.setNewSyncStateSetEpoch(replicasInfo.getSyncStateSetEpoch() + 1);
            response.setNewSyncStateSet(newSyncStateSet);
            final AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, newSyncStateSet);
            result.addEvent(event);

            return result;
        }
        response.setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
        return result;
    }

    public ControllerResult<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<ElectMasterResponseHeader> result = new ControllerResult<>(new ElectMasterResponseHeader());
        final ElectMasterResponseHeader response = result.getResponse();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final Set<String> syncStateSet = replicasInfo.getSyncStateSet();
            if (syncStateSet.size() > 1) {
                for (String replicas : syncStateSet) {
                    if (replicas.equals(replicasInfo.getMasterAddress())) {
                        continue;
                    }
                    // todo: check whether the replicas is active
                    response.setNewMasterAddress(replicas);
                    response.setMasterEpoch(replicasInfo.getMasterEpoch() + 1);

                    final ElectMasterEvent event = new ElectMasterEvent(brokerName, replicas);
                    result.addEvent(event);
                    return result;
                }
            }
            // If elect failed, we still need to apply an ElectMasterEvent to tell the statemachine
            // that the master was shutdown and no new master was elected.
            final ElectMasterEvent event = new ElectMasterEvent(false, brokerName);
            result.addEvent(event);
            response.setErrorCode(ErrorCodes.MASTER_NOT_AVAILABLE.getCode());
            return result;
        }
        result.getResponse().setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
        return result;
    }

    public ControllerResult<RegisterBrokerResponseHeader> registerBroker(final RegisterBrokerRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final String brokerAddress = request.getBrokerAddress();
        final ControllerResult<RegisterBrokerResponseHeader> result = new ControllerResult<>(new RegisterBrokerResponseHeader());
        final RegisterBrokerResponseHeader response = result.getResponse();
        boolean canBeElectedAsMaster = false;
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            if (replicasInfo.isMasterAlive()) {
                // If the master is alive, Check whether we need to apply an id for this replicas.
                long brokerId = 0;
                if (!brokerIdTable.containsKey(brokerAddress)) {
                    brokerId = brokerInfo.newBrokerId();
                    final ApplyBrokerIdEvent applyIdEvent = new ApplyBrokerIdEvent(request.getBrokerName(),
                        brokerAddress, brokerId);
                    result.addEvent(applyIdEvent);
                } else {
                    brokerId = brokerIdTable.get(brokerAddress);
                }
                response.setMasterAddress(replicasInfo.getMasterAddress());
                response.setMasterEpoch(replicasInfo.getMasterEpoch());
                response.setBrokerId(brokerId);
                return result;
            } else {
                // If the master is not alive, we should elect a new master:
                // Case1: This replicas was in sync state set list
                // Case2: The option {EnableElectUncleanMaster} is true
                canBeElectedAsMaster = replicasInfo.getSyncStateSet().contains(brokerAddress) || this.enableElectUncleanMaster;
            }
        } else {
            // If the broker's metadata does not exist in the state machine, the replicas can be elected as master directly.
            canBeElectedAsMaster = true;
        }
        if (canBeElectedAsMaster) {
            response.setMasterAddress(request.getBrokerAddress());
            int masterEpoch = this.inSyncReplicasInfoTable.containsKey(brokerName) ?
                this.inSyncReplicasInfoTable.get(brokerName).getMasterEpoch() + 1 : 1;
            response.setMasterEpoch(masterEpoch);

            final ElectMasterEvent event = new ElectMasterEvent(true, brokerName, brokerAddress, request.getClusterName());
            result.addEvent(event);
            return result;
        }
        result.getResponse().setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
        return result;
    }

    public ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<GetReplicaInfoResponseHeader> result = new ControllerResult<>(new GetReplicaInfoResponseHeader());
        final GetReplicaInfoResponseHeader response = result.getResponse();
        if (isContainsBroker(brokerName)) {
            // If exist broker metadata, just return metadata
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            response.setMasterAddress(replicasInfo.getMasterAddress());
            response.setMasterEpoch(replicasInfo.getMasterEpoch());
            response.setSyncStateSet(replicasInfo.getSyncStateSet());
            response.setSyncStateSetEpoch(replicasInfo.getSyncStateSetEpoch());
            return result;
        }
        result.getResponse().setErrorCode(ErrorCodes.INVALID_REQUEST.getCode());
        return result;
    }

    /********************************    The following methods will update statemachine   ********************************/

    // Apply events to memory statemachine.
    public void applyEvent(final EventMessage event) {
        final EventType type = event.getEventType();
        switch (type) {
            case ALTER_SYNC_STATE_SET_EVENT:
                handleAlterSyncStateSet((AlterSyncStateSetEvent) event);
                break;
            case APPLY_BROKER_ID_EVENT:
                handleApplyBrokerId((ApplyBrokerIdEvent) event);
                break;
            case ELECT_MASTER_EVENT:
                handleElectMaster((ElectMasterEvent) event);
                break;
            default:
                break;
        }
    }

    private void handleAlterSyncStateSet(final AlterSyncStateSetEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            replicasInfo.updateSyncStateSetInfo(event.getNewSyncStateSet());
        }
    }

    private void handleApplyBrokerId(final ApplyBrokerIdEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            if (!brokerIdTable.containsKey(event.getBrokerAddress())) {
                brokerIdTable.put(event.getBrokerAddress(), event.getNewBrokerId());
            }
        }
    }

    private void handleElectMaster(final ElectMasterEvent event) {
        final String brokerName = event.getBrokerName();
        final String newMaster = event.getNewMasterAddress();
        if (isContainsBroker(brokerName)) {
            final InSyncReplicasInfo replicasInfo = this.inSyncReplicasInfoTable.get(brokerName);
            final BrokerIdInfo brokerInfo = this.replicaInfoTable.get(brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();

            if (event.getNewMasterElected()) {
                // Step1, change the origin master to follower
                final String originMaster = replicasInfo.getMasterAddress();
                final long originMasterId = replicasInfo.getMasterOriginId();
                if (originMasterId > 0) {
                    brokerIdTable.put(originMaster, originMasterId);
                }

                // Step2, record new master
                final Long newMasterOriginId = brokerIdTable.get(newMaster);
                brokerIdTable.put(newMaster, LEADER_ID);
                replicasInfo.updateMasterInfo(newMaster, newMasterOriginId);

                // Step3, record new newSyncStateSet list
                final HashSet<String> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(newMaster);
                replicasInfo.updateSyncStateSetInfo(newSyncStateSet);
            } else {
                // If new master was not elected, which means old master was shutdown and the newSyncStateSet list had no more replicas
                // So we should delete old master, but retain newSyncStateSet list.
                final String originMaster = replicasInfo.getMasterAddress();
                final long originMasterId = replicasInfo.getMasterOriginId();
                if (originMasterId > 0) {
                    brokerIdTable.put(originMaster, originMasterId);
                }

                replicasInfo.updateMasterInfo("", -1);
            }
        } else {
            // When the first replicas of a broker come online,
            // we can create memory meta information for the broker, and regard it as master
            final String clusterName = event.getClusterName();
            final BrokerIdInfo brokerInfo = new BrokerIdInfo(clusterName, brokerName);
            final HashMap<String, Long> brokerIdTable = brokerInfo.getBrokerIdTable();
            final InSyncReplicasInfo replicasInfo = new InSyncReplicasInfo(clusterName, brokerName, newMaster);
            brokerIdTable.put(newMaster, LEADER_ID);
            this.inSyncReplicasInfoTable.put(brokerName, replicasInfo);
            this.replicaInfoTable.put(brokerName, brokerInfo);
        }
    }

    /********************************    Util methods   ********************************/

    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.inSyncReplicasInfoTable.containsKey(brokerName);
    }
}
