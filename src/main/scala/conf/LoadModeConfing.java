package conf;

import dao.AnalsmodelDao;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定期加载模型配置
 */
public class LoadModeConfing {

  public static void load() {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(new LoadRunable(),0,3, TimeUnit.MINUTES);
  }

  public static class LoadRunable implements Runnable {
    public void run() {
      try {
        AnalsmodelDao.loadAll();
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
  }
}
