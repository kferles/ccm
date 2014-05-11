package lockmanager;

import xaction.Xaction;

public class WaitObject {

    private final Xaction waitingXaction;

    private boolean cont = false, restart = false;

    public WaitObject(Xaction waitingXaction){
        this.waitingXaction = waitingXaction;
    }

    public synchronized void setContinue(){
        this.cont = true;
    }

    public synchronized void setRestart(){
        this.restart = true;
    }

    public synchronized Lock getMode(){
        return waitingXaction.getLockingMode();
    }

    public synchronized void waitForLock(){
        while(!cont && !restart){
            try {
                wait();
            } catch (InterruptedException _) {

            }
        }

        if(restart){
            //TODO: restart xaction
        }
    }

    public synchronized void notifyWaitingXaction(){
        notify();
    }

    public Xaction getWaitingXaction(){
        return this.waitingXaction;
    }
}
