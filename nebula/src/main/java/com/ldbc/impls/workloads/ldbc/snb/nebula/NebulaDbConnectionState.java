package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.ldbc.impls.workloads.ldbc.snb.BaseDbConnectionState;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class NebulaDbConnectionState  extends BaseDbConnectionState<NebulaQueryStore> {

    protected final NebulaPool pool;

    private final ThreadLocal<Session> session = new ThreadLocal<>();

    private final String user;

    private final String password;
    public NebulaDbConnectionState(Map<String, String> properties, NebulaQueryStore queryStore) throws UnknownHostException {
        super(properties, queryStore);

        user = properties.get("user");
        password = properties.get("password");
        String endPoint = properties.get("endpoint");

        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(500);
        List<HostAddress> addresses = new ArrayList<>();
        for (String ipPort : endPoint.split(",")) {
            String[] ipPortPair = ipPort.split(":");
            addresses.add(new HostAddress(ipPortPair[0], Integer.parseInt(ipPortPair[1])));
        }

        pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);
    }

    public Session getSession() {
        if (session.get() == null) {
            try {
                session.set(pool.getSession(user, password, false));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return session.get();
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }
}
