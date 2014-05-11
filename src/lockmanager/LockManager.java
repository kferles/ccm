package lockmanager;

import file.blockfile.BlockFile;
import util.Pair;
import xaction.Xaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LockManager {

    private static LockManager ourInstance = new LockManager();

    public static LockManager getInstance(){
        return ourInstance;
    }

    Map<Pair<BlockFile, Integer>, LockMode> lockTable = new HashMap<>();

    Map<Pair<BlockFile,Integer>, List<WaitObject>> waitTable = new HashMap<>();

    public void getLock(BlockFile bf, Integer num){
        Xaction xaction = Xaction.getExecutingXaction();
        Pair<BlockFile, Integer> lockRequest = new Pair<>(bf, num);
        WaitObject waitObj = null;

        assert xaction != null;

        synchronized (this){
            if(!lockTable.containsKey(lockRequest))
                lockTable.put(lockRequest, new LockMode());

            LockMode currMode = lockTable.get(lockRequest);

            if(!currMode.canProceed(xaction)){
                waitObj = new WaitObject(xaction);

                if(!waitTable.containsKey(lockRequest))
                    waitTable.put(lockRequest, new ArrayList<WaitObject>());

                waitTable.get(lockRequest).add(waitObj);
            }
        }

        if(waitObj != null)
            waitObj.waitForLock();
    }

    public synchronized void releaseLock(BlockFile bf, Integer num){
        Xaction xaction = Xaction.getExecutingXaction();
        Pair<BlockFile, Integer> lockRelease = new Pair<>(bf, num);

        assert lockTable.containsKey(lockRelease);

        LockMode mode = lockTable.get(lockRelease);

        Lock acqLockMode = mode.removeXaction(xaction);

        if(mode.xactionList.isEmpty() && waitTable.containsKey(lockRelease)){
            List<WaitObject> waitingXactions = waitTable.get(lockRelease);

            if(waitingXactions.size() > 0){
                if(acqLockMode == Lock.X){
                    WaitObject currWait = waitingXactions.get(0);
                    Lock currMode = currWait.getMode();
                    Xaction currWaitingXaction = currWait.getWaitingXaction();

                    if(currMode == Lock.X){
                        mode.addXaction(currWaitingXaction);
                        waitingXactions.remove(0);
                        currWait.setContinue();
                        currWait.notifyWaitingXaction();
                        return;
                    }

                    do{
                        mode.addXaction(currWaitingXaction);
                        waitingXactions.remove(0);
                        currWait.setContinue();
                        currWait.notifyWaitingXaction();

                        currWait = waitingXactions.get(0);
                        currWaitingXaction = currWait.getWaitingXaction();
                    }while(currWait.getMode() != Lock.X);
                }
                else{
                    WaitObject headWait = waitingXactions.get(0);
                    assert headWait.getMode() == Lock.X;

                    mode.addXaction(headWait.getWaitingXaction());
                    waitingXactions.remove(0);
                    headWait.setContinue();
                    headWait.notifyWaitingXaction();
                }
            }
        }
    }

    public void updateLock(BlockFile bf, Integer num){
        Xaction currXaction = Xaction.getExecutingXaction();
        Pair<BlockFile, Integer> updateLock = new Pair<>(bf, num);
        WaitObject waitObj = null;

        synchronized(this){
            assert lockTable.containsKey(updateLock);

            LockMode mode = lockTable.get(updateLock);

            assert mode.xactionList.contains(new Pair<>(currXaction, Lock.SIX));

            if(mode.xactionList.size() == 1){
                mode.addXaction(currXaction);
            }
            else{
                waitObj = new WaitObject(currXaction);

                if(!waitTable.containsKey(updateLock))
                    waitTable.put(updateLock, new ArrayList<WaitObject>());

                List<WaitObject> waitObjs = waitTable.get(updateLock);

                //TODO: should we give priority to updaters?
                waitObjs.add(waitObj);
            }
        }

        if(waitObj != null)
            waitObj.waitForLock();
    }

}
