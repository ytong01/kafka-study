package cn.cloudwalk.demo05.ignite;

public class IgniteRepositoryFactory {

    private static IgniteRepository repository;

    public synchronized static IgniteRepository getRepository() {
        if (repository == null) {
            repository = new IgniteRepository();
            repository.init();
        }
        return repository;
    }
}
