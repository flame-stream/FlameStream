package experiments.nikita.akka.cluster;

import experiments.nikita.akka.cluster.front.FrontEndApp;
import experiments.nikita.akka.cluster.master.MasterApp;
import experiments.nikita.akka.cluster.worker.WorkerApp;

/**
 * Created by marnikitta on 2/2/17.
 */
public class Application {
  public static void main(String[] args) {
    FrontEndApp.main(new String[]{"2541", "2542", "2543", "2544"});
    WorkerApp.main(new String[]{"2531", "2532", "2533", "2534"});
    MasterApp.main(new String[]{"2551"});
  }
}
