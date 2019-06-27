package cn.cloudwalk.demo05;

import cn.cloudwalk.demo05.ignite.NetworkSignalIgniteRepository;

import java.util.Arrays;
import java.util.List;

public class IgniteStreamingSQLQuery implements Runnable {

    private static final long POLL_TIMEOUT = 3000;
    private static final List<String> QUERIES
            = Arrays.asList("SELECT deviceId, SUM(rxData) AS rxTotal, " + "SUM(txData) AS txTOTAL FROM NetworkSignalDomain " + "GROUP BY deviceId ORDER BY rxTotal DESC, txTotal DESC LIMIT 5",
            "SELECT networkType, SUM(rxData) AS rxTotal, SUM(txData) AS txTotal " + "FROM NetworkSignalDomain GROUP BY networkType",
            "SELECT networkType, AVG(rxSpeed) AS avgRxSpeed, AVG(txSpeed) AS avgTxSpeed" + " FROM NetworkSignalDomain GROUP BY networkType");

    @Override
    public void run() {

        NetworkSignalIgniteRepository repository = new NetworkSignalIgniteRepository();
        List<List<?>> rows = null;
        try{
            do {
                try {
                    Thread.sleep(POLL_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                for (String sql : QUERIES) {
                    rows = repository.sqlQuery(sql);
                    for (List<?> row : rows) {
                        System.out.println(row.toString());
                    }
                }
            } while (rows != null && rows.size() > 0);
        } finally {
            repository.close();
        }
    }
}
