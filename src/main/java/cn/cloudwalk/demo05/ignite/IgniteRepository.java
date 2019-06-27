package cn.cloudwalk.demo05.ignite;

import cn.cloudwalk.demo05.config.ConfigUtil;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteRepository {

    private Ignite ignite;

    void init() {
        ignite = Ignition.start(ConfigUtil.getResource("ignite.xml"));
    }

    IgniteCache getCache(CacheConfiguration configuration) {
//        if (ignite == null) {
//            init();
//        }
        return ignite.getOrCreateCache(configuration);
    }

    public void close() {
        if (ignite != null) {
            ignite.close();
        }
    }
}
