package lockmanager;

import java.util.ArrayList;
import java.util.List;

import util.Pair;
import xaction.Xaction;

public class LockMode {

    //TODO: we can easily convert this guy to a Map
    List<Pair<Xaction, Lock>> xactionList = new ArrayList<>();

    synchronized boolean canProceed(Xaction currXaction){
        Lock lockMode = currXaction.getLockingMode();
        boolean rv = true;

        if(lockMode == Lock.X){
            rv = xactionList.size() == 0;
        }
        else{
            if(xactionList.size() == 1){
                Pair<Xaction, Lock> owningXaction = xactionList.get(0);
                rv = owningXaction.second != Lock.X;
            }
        }

        if(rv){
            xactionList.add(new Pair<>(currXaction, lockMode));
        }

        return rv;
    }

    private Pair<Xaction, Lock> isInUpdateMode(Xaction xaction){
        Pair<Xaction, Lock> key1 = new Pair<>(xaction, Lock.SIX);
        Pair<Xaction, Lock> key2 = new Pair<>(xaction, Lock.S);
        if(xactionList.contains(key1)){
            return xactionList.get(xactionList.indexOf(key1));
        }
        if(xactionList.contains(key2)){
            return xactionList.get(xactionList.indexOf(key2));
        }
        return null;
    }

    synchronized void addXaction(Xaction xaction){
        Pair<Xaction, Lock> updateMode;
        if((updateMode = isInUpdateMode(xaction)) != null){
            assert xaction.getLockingMode() == Lock.X;
            updateMode.second = Lock.X;
        }
        else{
            this.xactionList.add(new Pair<>(xaction, xaction.getLockingMode()));
        }
    }

    synchronized Lock removeXaction(Xaction xaction){
        for(Pair<Xaction, Lock> curr : xactionList){
            if(curr.first == xaction){
                xactionList.remove(curr);
                return curr.second;
            }
        }

        assert false;
        return null;
    }

}