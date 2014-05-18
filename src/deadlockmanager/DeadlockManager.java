package deadlockmanager;

import lockmanager.Lock;
import lockmanager.WaitObject;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.cycle.JohnsonSimpleCycles;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import server.CcmServer;
import util.Pair;
import xaction.Xaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeadlockManager extends Thread{

    private DeadlockManager(){ }

	private static DeadlockManager ourInstance = new DeadlockManager();

	public static DeadlockManager getInstance(){
		return ourInstance;
	}

    private boolean deadlockDetectionRunning = false;

	private DirectedGraph<Xaction, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
	// includes the transactions that wait
	private Map<Xaction, WaitObject> waitObjMap = new HashMap<>();

	public synchronized void addNewPart(List<Pair<Xaction, Lock>> xactionList, WaitObject waitobj) {
		Xaction waitXaction = waitobj.getWaitingXaction();

		if(!waitObjMap.containsKey(waitXaction)) {
			waitObjMap.put(waitXaction, waitobj);
			if(!graph.containsVertex(waitXaction)) 
			    graph.addVertex(waitXaction);
		}

		for(Pair<Xaction, Lock> pair : xactionList) {
            Xaction waitingFromXaction = pair.first;
            if(!graph.containsVertex(waitingFromXaction))
	    		graph.addVertex(waitingFromXaction);

			if(waitXaction != waitingFromXaction &&
               !graph.containsEdge(waitXaction, waitingFromXaction))
                graph.addEdge(waitXaction, waitingFromXaction);
	    }
    }

	public synchronized void removeWaitPart(List<Pair<Xaction, Lock>> xactionList, WaitObject waitobj) {
		Xaction waitXaction = waitobj.getWaitingXaction();

        assert waitObjMap.containsKey(waitXaction);

		for(Pair<Xaction, Lock> pair : xactionList){
            if(waitXaction != pair.first){
                DefaultEdge edge = graph.removeEdge(waitXaction, pair.first);

                assert edge != null;

                if((graph.inDegreeOf(pair.first) == 0) && (graph.outDegreeOf(pair.first) == 0))
                    graph.removeVertex(pair.first);
            }
		}

		waitObjMap.remove(waitXaction);
		graph.removeVertex(waitXaction);
	}

	public synchronized void removeVertexPart(Xaction xaction) {

		// removes and its edges automatically
        waitObjMap.remove(xaction);
		graph.removeVertex(xaction);
	}

	private synchronized void detectAllCycles() {
        deadlockDetectionRunning = true;
		// based on Johnson's algorithm for cycles in a graph
		JohnsonSimpleCycles<Xaction, DefaultEdge> detector = new JohnsonSimpleCycles<>(this.graph);

		List<List<Xaction>> cycleList = detector.findSimpleCycles();

		if(cycleList.isEmpty()){
            deadlockDetectionRunning = false;
            notifyAll();
			return;
        }

		List<Xaction> removeList = new ArrayList<>();

        for(List<Xaction> list : cycleList) {
			for(Xaction xact : list) {
				// if the xact has been notified, there is no cycle any more
				if(removeList.contains(xact)) {
					break;
				}
				if(waitObjMap.containsKey(xact)) {
                    WaitObject waitObj = waitObjMap.remove(xact);
                    removeList.add(waitObj.getWaitingXaction());
					waitObj.setRestart();
					waitObj.notifyWaitingXaction();

					break;
				}
			}
		}

        deadlockDetectionRunning = false;
        notifyAll();
	}

    public synchronized void waitForDeadlockManager(){
        while(deadlockDetectionRunning)
            try {
                wait();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
    }

	@Override
	public void run() {
		while(!CcmServer.stop) {
            detectAllCycles();
			try {
				sleep(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

}
