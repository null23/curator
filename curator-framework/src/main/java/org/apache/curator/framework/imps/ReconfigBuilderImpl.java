/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.imps;

import com.google.common.collect.ImmutableList;
import org.apache.curator.RetryLoop;
import org.apache.curator.TimeTrace;
import org.apache.curator.framework.api.*;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class ReconfigBuilderImpl implements
    ReconfigBuilder,
    ReconfigBuilderMain,
    StatEnsembleable<byte[]>,
    Configurable,
    StatConfigEnsembleable,
    BackgroundOperation<Void>
{
    private final CuratorFrameworkImpl client;

    private Backgrounding backgrounding = new Backgrounding();
    private Stat responseStat;
    private long fromConfig = -1;
    private List<String> adding;
    private List<String> joining;
    private List<String> leaving;

    public ReconfigBuilderImpl(CuratorFrameworkImpl client)
    {
        this.client = client;
    }

    @Override
    public byte[] forEnsemble() throws Exception
    {
        if ( backgrounding.inBackground() )
        {
            client.processBackgroundOperation(new OperationAndData<>(this, null, backgrounding.getCallback(), null, backgrounding.getContext()), null);
            return new byte[0];
        }
        else
        {
            return ensembleInForeground();
        }
    }

    @Override
    public Ensembleable<byte[]> storingStatIn(Stat stat)
    {
        responseStat = stat;
        return this;
    }

    @Override
    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
    {
        fromConfig = config;
        return this;
    }

    @Override
    public JoinLeaveStatConfigEnsembleable adding(String... server)
    {
        return adding((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public JoinLeaveStatConfigEnsembleable adding(List<String> servers)
    {
        this.adding = (servers != null) ? ImmutableList.copyOf(servers) : ImmutableList.<String>of();

        return new JoinLeaveStatConfigEnsembleable()
        {
            @Override
            public byte[] forEnsemble() throws Exception
            {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public Ensembleable<byte[]> storingStatIn(Stat stat)
            {
                return ReconfigBuilderImpl.this.storingStatIn(stat);
            }

            @Override
            public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
            {
                return ReconfigBuilderImpl.this.fromConfig(config);
            }

            @Override
            public LeaveStatConfigEnsembleable joining(String... server)
            {
                return joining((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public LeaveStatConfigEnsembleable joining(List<String> servers)
            {
                return new LeaveStatConfigEnsembleable()
                {
                    @Override
                    public byte[] forEnsemble() throws Exception
                    {
                        return ReconfigBuilderImpl.this.forEnsemble();
                    }

                    @Override
                    public Ensembleable<byte[]> storingStatIn(Stat stat)
                    {
                        return ReconfigBuilderImpl.this.storingStatIn(stat);
                    }

                    @Override
                    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
                    {
                        return ReconfigBuilderImpl.this.fromConfig(config);
                    }

                    @Override
                    public StatConfigEnsembleable leaving(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.leaving(servers);
                    }

                    @Override
                    public StatConfigEnsembleable leaving(String... server)
                    {
                        return ReconfigBuilderImpl.this.leaving(server);
                    }
                };
            }

            @Override
            public JoinStatConfigEnsembleable leaving(String... server)
            {
                return leaving((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public JoinStatConfigEnsembleable leaving(List<String> servers)
            {
                return new JoinStatConfigEnsembleable()
                {
                    @Override
                    public byte[] forEnsemble() throws Exception
                    {
                        return ReconfigBuilderImpl.this.forEnsemble();
                    }

                    @Override
                    public Ensembleable<byte[]> storingStatIn(Stat stat)
                    {
                        return ReconfigBuilderImpl.this.storingStatIn(stat);
                    }

                    @Override
                    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
                    {
                        return ReconfigBuilderImpl.this.fromConfig(config);
                    }

                    @Override
                    public StatConfigEnsembleable joining(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.joining(servers);
                    }

                    @Override
                    public StatConfigEnsembleable joining(String... server)
                    {
                        return ReconfigBuilderImpl.this.joining(server);
                    }
                };
            }
        };
    }

    @Override
    public ReconfigBuilderMain inBackground()
    {
        backgrounding = new Backgrounding(true);
        return this;
    }

    @Override
    public ReconfigBuilderMain inBackground(Object context)
    {
        backgrounding = new Backgrounding(context);
        return this;
    }

    @Override
    public ReconfigBuilderMain inBackground(BackgroundCallback callback)
    {
        backgrounding = new Backgrounding(callback);
        return this;
    }

    @Override
    public ReconfigBuilderMain inBackground(BackgroundCallback callback, Object context)
    {
        backgrounding = new Backgrounding(callback, context);
        return this;
    }

    @Override
    public ReconfigBuilderMain inBackground(BackgroundCallback callback, Executor executor)
    {
        backgrounding = new Backgrounding(callback, executor);
        return this;
    }

    @Override
    public ReconfigBuilderMain inBackground(BackgroundCallback callback, Object context, Executor executor)
    {
        backgrounding = new Backgrounding(client, callback, context, executor);
        return this;
    }

    @Override
    public LeaveAddStatConfigEnsembleable joining(String... server)
    {
        return joining((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public LeaveAddStatConfigEnsembleable joining(List<String> servers)
    {
        joining = (servers != null) ? ImmutableList.copyOf(servers) : ImmutableList.<String>of();

        return new LeaveAddStatConfigEnsembleable()
        {
            @Override
            public byte[] forEnsemble() throws Exception
            {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public Ensembleable<byte[]> storingStatIn(Stat stat)
            {
                return ReconfigBuilderImpl.this.storingStatIn(stat);
            }

            @Override
            public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
            {
                return ReconfigBuilderImpl.this.fromConfig(config);
            }

            @Override
            public LeaveStatConfigEnsembleable adding(String... server)
            {
                return adding((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public LeaveStatConfigEnsembleable adding(List<String> servers)
            {
                return new LeaveStatConfigEnsembleable()
                {
                    @Override
                    public byte[] forEnsemble() throws Exception
                    {
                        return ReconfigBuilderImpl.this.forEnsemble();
                    }

                    @Override
                    public Ensembleable<byte[]> storingStatIn(Stat stat)
                    {
                        return ReconfigBuilderImpl.this.storingStatIn(stat);
                    }

                    @Override
                    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
                    {
                        return ReconfigBuilderImpl.this.fromConfig(config);
                    }

                    @Override
                    public StatConfigEnsembleable leaving(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.leaving(servers);
                    }

                    @Override
                    public StatConfigEnsembleable leaving(String... server)
                    {
                        return ReconfigBuilderImpl.this.leaving(server);
                    }
                };
            }

            @Override
            public AddStatConfigEnsembleable leaving(String... server)
            {
                return leaving((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public AddStatConfigEnsembleable leaving(List<String> servers)
            {
                return new AddStatConfigEnsembleable()
                {
                    @Override
                    public byte[] forEnsemble() throws Exception
                    {
                        return ReconfigBuilderImpl.this.forEnsemble();
                    }

                    @Override
                    public Ensembleable<byte[]> storingStatIn(Stat stat)
                    {
                        return ReconfigBuilderImpl.this.storingStatIn(stat);
                    }

                    @Override
                    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
                    {
                        return ReconfigBuilderImpl.this.fromConfig(config);
                    }

                    @Override
                    public StatConfigEnsembleable adding(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.adding(servers);
                    }

                    @Override
                    public StatConfigEnsembleable adding(String... server)
                    {
                        return ReconfigBuilderImpl.this.adding(server);
                    }
                };
            }
        };
    }

    @Override
    public JoinAddStatConfigEnsembleable leaving(String... server)
    {
        return leaving((server != null) ? Arrays.asList(server) : null);
    }

    @Override
    public JoinAddStatConfigEnsembleable leaving(List<String> servers)
    {
        leaving = (servers != null) ? ImmutableList.copyOf(servers) : ImmutableList.<String>of();

        return new JoinAddStatConfigEnsembleable()
        {
            @Override
            public byte[] forEnsemble() throws Exception
            {
                return ReconfigBuilderImpl.this.forEnsemble();
            }

            @Override
            public Ensembleable<byte[]> storingStatIn(Stat stat)
            {
                return ReconfigBuilderImpl.this.storingStatIn(stat);
            }

            @Override
            public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
            {
                return ReconfigBuilderImpl.this.fromConfig(config);
            }

            @Override
            public JoinStatConfigurable adding(String... server)
            {
                return adding((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public JoinStatConfigurable adding(List<String> servers)
            {
                return new JoinStatConfigurable()
                {
                    @Override
                    public Configurable joining(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.joining(servers);
                    }

                    @Override
                    public Configurable joining(String... server)
                    {
                        return ReconfigBuilderImpl.this.joining(server);
                    }
                };
            }

            @Override
            public AddStatConfigEnsembleable joining(String... server)
            {
                return joining((server != null) ? Arrays.asList(server) : null);
            }

            @Override
            public AddStatConfigEnsembleable joining(List<String> servers)
            {
                return new AddStatConfigEnsembleable()
                {
                    @Override
                    public byte[] forEnsemble() throws Exception
                    {
                        return ReconfigBuilderImpl.this.forEnsemble();
                    }

                    @Override
                    public Ensembleable<byte[]> storingStatIn(Stat stat)
                    {
                        return ReconfigBuilderImpl.this.storingStatIn(stat);
                    }

                    @Override
                    public StatEnsembleable<byte[]> fromConfig(long config) throws Exception
                    {
                        return ReconfigBuilderImpl.this.fromConfig(config);
                    }

                    @Override
                    public StatConfigEnsembleable adding(List<String> servers)
                    {
                        return ReconfigBuilderImpl.this.adding(servers);
                    }

                    @Override
                    public StatConfigEnsembleable adding(String... server)
                    {
                        return ReconfigBuilderImpl.this.adding(server);
                    }
                };
            }
        };
    }

    @Override
    public void performBackgroundOperation(final OperationAndData<Void> data) throws Exception
    {
        final TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Background");
        AsyncCallback.DataCallback callback = new AsyncCallback.DataCallback()
        {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] bytes, Stat stat)
            {
                trace.commit();
                if ( (responseStat != null) && (stat != null) )
                {
                    DataTree.copyStat(stat, responseStat);
                }
                CuratorEvent event = new CuratorEventImpl(client, CuratorEventType.RECONFIG, rc, path, null, ctx, stat, bytes, null, null, null, null);
                client.processBackgroundOperation(data, event);
            }
        };
        client.getZooKeeper().reconfig(joining, leaving, adding, fromConfig, callback, backgrounding.getContext());
    }

    private byte[] ensembleInForeground() throws Exception
    {
        TimeTrace trace = client.getZookeeperClient().startTracer("ReconfigBuilderImpl-Foreground");
        byte[] responseData = RetryLoop.callWithRetry
            (
                client.getZookeeperClient(),
                new Callable<byte[]>()
                {
                    @Override
                    public byte[] call() throws Exception
                    {
                        return client.getZooKeeper().reconfig(joining, leaving, adding, fromConfig, responseStat);
                    }
                }
            );
        trace.commit();
        return responseData;
    }
}