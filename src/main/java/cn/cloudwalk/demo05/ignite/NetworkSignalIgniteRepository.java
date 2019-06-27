package cn.cloudwalk.demo05.ignite;

import cn.cloudwalk.demo05.domain.NetworkSignalDomain;
import cn.cloudwalk.demo05.model.NetworkSignal;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NetworkSignalIgniteRepository {

    private static final String CACHE = "NetworkSignal";
    private static final Duration DURATION = new Duration(TimeUnit.MINUTES, 15);

    private IgniteRepository repository;
    private IgniteCache<String, NetworkSignalDomain> cache;

    public NetworkSignalIgniteRepository() {

        repository = IgniteRepositoryFactory.getRepository();
        CacheConfiguration<String, NetworkSignal> config = new CacheConfiguration<>(CACHE);
        config.setCacheMode(CacheMode.REPLICATED);
        config.setIndexedTypes(String.class, NetworkSignalDomain.class);
        this.cache = repository.getCache(config).withExpiryPolicy(new CreatedExpiryPolicy(DURATION));
    }

    public void save(NetworkSignalDomain entity) {
        cache.put(entity.getId(), entity);
    }

    public List<List<?>> sqlQuery(String query) {
        SqlFieldsQuery sql = new SqlFieldsQuery(query);
        FieldsQueryCursor<List<?>> cursor = cache.query(sql);
        return cursor.getAll();
    }

    public void close() {
        repository.close();
    }
}
