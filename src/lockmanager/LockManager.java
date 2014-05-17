package lockmanager;

import file.blockfile.BlockFile;
import util.Pair;
import xaction.Xaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import deadlockmanager.DeadlockManager;

public class LockManager {

    private static LockManager ourInstance = new LockManager();

    public static LockManager getInstance(){
        return ourInstance;
    }

    Map<Pair<BlockFile, Integer>, LockMode> lockTable = new HashMap<>();

    Map<Pair<BlockFile,Integer>, List<WaitObject>> waitTable = new HashMap<>();

    DeadlockManager deadlock = DeadlockManager.getInstance();

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
                deadlock.addNewPart(currMode.xactionList, waitObj);
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
        deadlock.removeVertexPart(xaction);

        boolean notifyWaiters = false;
        if(waitTable.containsKey(lockRelease)){
            if(mode.xactionList.isEmpty())
                notifyWaiters = true;
            else if(mode.xactionList.size() == 1){
                Xaction waitingXaction = waitTable.get(lockRelease).get(0).getWaitingXaction();
                if(mode.xactionList.get(0).first.equals(waitingXaction))
                    notifyWaiters = true;
            }
        }

        if(notifyWaiters){
            List<WaitObject> waitingXactions = waitTable.get(lockRelease);

            if(waitingXactions.size() > 0){
                if(acqLockMode == Lock.X){
                    WaitObject currWait = waitingXactions.get(0);
                    Lock currMode = currWait.getMode();
                    Xaction currWaitingXaction = currWait.getWaitingXaction();

                    if(currMode == Lock.X){
                        mode.addXaction(currWaitingXaction);
                        waitingXactions.remove(0);
                        deadlock.removeWaitPart(mode.xactionList, currWait);

                        currWait.setContinue();
                        currWait.notifyWaitingXaction();
                        return;
                    }

                    do{
                        mode.addXaction(currWaitingXaction);
                        waitingXactions.remove(0);
                        deadlock.removeWaitPart(mode.xactionList, currWait);

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
                    deadlock.removeWaitPart(mode.xactionList, headWait);

                    headWait.setContinue();
                    headWait.notifyWaitingXaction();
                }
            }
        }
    }

    public void updateSIXLock(BlockFile bf, Integer num){
        Xaction currXaction = Xaction.getExecutingXaction();
        Pair<BlockFile, Integer> updateLock = new Pair<>(bf, num);
        WaitObject waitObj = null;

        synchronized(this){
            assert lockTable.containsKey(updateLock);

            LockMode mode = lockTable.get(updateLock);

            if(mode.xactionList.contains(new Pair<>(currXaction, Lock.X)))
                return;

            assert mode.xactionList.contains(new Pair<>(currXaction, Lock.SIX));

            if(mode.xactionList.size() == 1){
                mode.addXaction(currXaction);
            }
            else{
                waitObj = new WaitObject(currXaction);

                if(!waitTable.containsKey(updateLock))
                    waitTable.put(updateLock, new ArrayList<WaitObject>());

                List<WaitObject> waitObjs = waitTable.get(updateLock);

                /*
                 * If an updater has to wait, we put the xaction
                 * in the front of the list to be able to recognise
                 * it during release block.
                 */
                waitObjs.add(0, waitObj);
                deadlock.addNewPart(mode.xactionList, waitObj);
            }
        }

        if(waitObj != null)
            waitObj.waitForLock();
    }

    public void updateSLock(BlockFile bf, Integer num){
        Xaction currXaction = Xaction.getExecutingXaction();
        Pair<BlockFile, Integer> updateLock = new Pair<>(bf, num);
        Lock newMode = currXaction.getLockingMode();
        WaitObject waitObj = null;

        synchronized(this){
            assert lockTable.containsKey(updateLock);

            LockMode mode = lockTable.get(updateLock);

            Pair<Xaction, Lock> keyLock = new Pair<>(currXaction, Lock.S);
            assert mode.xactionList.contains(keyLock);

            if(newMode == Lock.SIX){
                mode.xactionList.get(mode.xactionList.indexOf(keyLock)).second = Lock.SIX;
            }
            else{
                assert newMode == Lock.X;
                if(mode.xactionList.size() == 1){
                    mode.addXaction(currXaction);
                }
                else{
                    waitObj = new WaitObject(currXaction);

                    if(!waitTable.containsKey(updateLock))
                        waitTable.put(updateLock, new ArrayList<WaitObject>());

                    List<WaitObject> waitObjs = waitTable.get(updateLock);

                    /*
                     * If an updater has to wait, we put the xaction
                     * in the front of the list to be able to recognise
                     * it during release block.
                     */
                    waitObjs.add(0, waitObj);
                    deadlock.addNewPart(mode.xactionList, waitObj);
                }
            }
        }

        if(waitObj != null)
            waitObj.waitForLock();
    }

    public synchronized Lock getModeFor(BlockFile bf, Integer num){
        Xaction xaction = Xaction.getExecutingXaction();

        Pair<BlockFile, Integer> key = new Pair<>(bf, num);
        assert lockTable.containsKey(key);

        LockMode mode = lockTable.get(key);
        Lock rv = null;
        for(Pair<Xaction, Lock> owningXaction : mode.xactionList){
            if(owningXaction.first.equals(xaction)){
                rv = owningXaction.second;
            }
        }
        assert rv != null;
        return rv;
    }

}
