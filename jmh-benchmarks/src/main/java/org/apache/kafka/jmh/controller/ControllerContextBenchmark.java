/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.controller.ControllerContext;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.controller.ReplicaAssignment;
import scala.jdk.javaapi.CollectionConverters;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ControllerContextBenchmark {
    @Param("200")
    int numBrokers;

    @Param("50")
    int numPartitions;

    private ControllerContext context;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        context = new ControllerContext();
        for (int i = 0; i < numBrokers; i++) {
            int brokerId = i;
            context.addLiveBrokers(CollectionConverters.asScala(new HashMap<Broker, Object>() {{
                put(new Broker(brokerId, "localhost", 0, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT), (Object) 1L);
            }}));
        }
        for (int i = 0; i < numPartitions; i++) {
            // distribute evenly
            int leader = i % numBrokers;
            int f1 = (leader + 1) % numBrokers;
            int f2 = (leader + 2) % numBrokers;

            context.updatePartitionFullReplicaAssignment(
                    new TopicPartition("topic", i),
                    ReplicaAssignment.apply(CollectionConverters.asScala(
                            Arrays.asList(leader, f1, (Object) f2)
                    ).toSeq())
            );
            context.putPartitionLeadershipInfo(
                    new TopicPartition("topic", i),
                    new LeaderIsrAndControllerEpoch(new LeaderAndIsr(
                            leader, 1, LeaderRecoveryState.RECOVERED,
                            CollectionConverters.asScala(Arrays.asList(
                                    new BrokerState().setBrokerId(leader),
                                    new BrokerState().setBrokerId(f1),
                                    new BrokerState().setBrokerId(f2)
                            )).toList(), 1), 1));
        }
    }

    @Benchmark
    public void testIsReplicaOnline() {
        context.isReplicaOnline(0, new TopicPartition("topic", 0));
    }
}
