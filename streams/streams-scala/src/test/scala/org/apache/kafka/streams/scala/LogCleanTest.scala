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
package org.apache.kafka.streams.scala

import kafka.log.LogCleanerManager
import kafka.server.ReplicaFetcherThread

import java.util.{Optional, Properties}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic}
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Tag

import java.io.File

@Tag("integration")
class LogCleanTest {

  private val cluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(4, {
    val props = new Properties()
    props
  })

  final private val alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000
  private val mockTime: MockTime = cluster.time
  mockTime.setCurrentTimeMs(alignedTime)

  private val testFolder: File = TestUtils.tempDirectory()

  @BeforeEach
  def startKafkaCluster(): Unit = {
    cluster.start()
  }

  @AfterEach
  def stopKafkaCluster(): Unit = {
    cluster.stop()
    Utils.delete(testFolder)
  }

  @Test
  def test(): Unit = {
    val tp = new TopicPartition("test-topic", 0)

    val admin = Admin.create({
      val props = new Properties()
      props.put("bootstrap.servers", cluster.bootstrapServers())
      props
    })

    for (i <- 0 until 4) {
      println(s"broker ${i} - ${cluster.broker(i).logDir()}")
    }

    val newTopic = new NewTopic(tp.topic(), java.util.Map.of(
      0, java.util.List.of(0, 1, 2)
    )).configs(java.util.Map.of(
      "cleanup.policy", "compact",
      "min.insync.replicas", "2",
      "segment.ms", "5000"
    ))
    admin.createTopics(java.util.Set.of(newTopic)).all().get()

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)
    producer.send(new ProducerRecord(tp.topic(), "k1", "val"))
    producer.send(new ProducerRecord(tp.topic(), "k2", "val"))
    producer.send(new ProducerRecord(tp.topic(), "k2", null)).get()

    // wait for a while until the segment can be rolled
    Thread.sleep(10000)

    ReplicaFetcherThread.sleepMs.set(10000)

    // initiate roll
    producer.send(new ProducerRecord(tp.topic(), "k3", null))

    Thread.sleep(5000)

    TestUtils.waitForCondition(() => LogCleanerManager.cleanCount.get() > 0, 3000L, "wait done cleaning")

//    admin.alterPartitionReassignments(java.util.Map.of(
//      tp, Optional.of(new NewPartitionReassignment(java.util.List.of(1, 0, 2)))
//    )).all().get()
//    Thread.sleep(500)
//    admin.electLeaders(ElectionType.PREFERRED, java.util.Set.of(tp)).all().get()

    ReplicaFetcherThread.sleepMs.set(0)

    Thread.sleep(10000)

    // check the suffix is not truncated
    assertEquals(3, producer.send(new ProducerRecord(tp.topic(), "k3", "val")).get().offset())
  }
}
