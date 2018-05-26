/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
 
 package com.soteradefense.dga.graphx.neighboringcommunity

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.ObjectArraySerializer
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.soteradefense.dga.graphx.harness.Harness
import com.soteradefense.dga.graphx.neighboringcommunity.louvain.NeighboringCommunityLouvainRunner
import com.soteradefense.dga.graphx.neighboringcommunity.wcc.NeighboringCommunityWCCRunner
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

abstract class AbstractNeighboringCommunityRunner(var minimumCompressionProgress: Int, var progressCounter: Int, protected var qValues: Array[(Int, Double)]) extends Harness with Serializable with KryoSerializable {

  /**
   * The run return type is the save return type for this particular runner.
   */
  override type R = S

  /**
   * Run method for an analytic.
   *
   * @param sc The current spark context.
   * @param graph A Graph object with an optional edge weight.
   * @tparam VD ClassTag for the vertex data type.
   * @return The type of R
   */
  override def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): R = {
    val wccRunner: NeighboringCommunityWCCRunner = new NeighboringCommunityWCCRunner
    val louvainRunner: NeighboringCommunityLouvainRunner = new NeighboringCommunityLouvainRunner(minimumCompressionProgress, progressCounter)
    val wccResult = wccRunner.run(sc, graph).cache()
    val louvainResult = louvainRunner.run(sc, graph).cache()
    val joinGraph = wccResult.outerJoinVertices(louvainResult)(
      (vertexId, componentId, communityInfo) => {
        (componentId, communityInfo.get)
      }).cache()
    val finalGraph = joinGraph.mapTriplets(triplet => {
      val srcCommunity = triplet.srcAttr._2
      val dstCommunity = triplet.dstAttr._2
      srcCommunity != dstCommunity
    }).cache()
    save(finalGraph)
  }

  /**
   * Method for saving the results of an analytic.
   *
   * @param graph A graph of any type.
   * @tparam VD ClassTag for the vertex data.
   * @tparam ED ClassTag for the edge data.
   * @return The type of S
   */
  override def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): S

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, this.minimumCompressionProgress)
    kryo.writeObject(output, this.progressCounter)
    val objectArraySerializer = new ObjectArraySerializer(kryo, output.getClass())
    objectArraySerializer.write(kryo, output, this.qValues.asInstanceOf[Array[Object]])
  }


  override def read(kryo: Kryo, input: Input): Unit = {
    this.minimumCompressionProgress = kryo.readObject(input, classOf[Int])
    this.progressCounter = kryo.readObject(input, classOf[Int])
    val objectArraySerializer = new ObjectArraySerializer(kryo, input.getClass())
    this.qValues = objectArraySerializer.read(kryo, input, classOf[Array[Object]]).asInstanceOf[Array[(Int, Double)]]
  }
}
