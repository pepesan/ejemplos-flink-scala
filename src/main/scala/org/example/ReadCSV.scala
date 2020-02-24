package org.example

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */
object ReadCSV {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[String] = env.readTextFile("resources/gold_price.csv")
    println(ds.count())
    var mapped = ds.map(_.toLowerCase)
    println(mapped.count())
    mapped = ds.map((line) => line)
    println(mapped.count())
    mapped = ds.map((line)=>{
      println(line)
      line.toLowerCase
    } )
    println(mapped.count())
    val dividido= ds.map((line)=>{
      println(line.split(","))
      line.split(",")
    } )
    println(dividido.count())
    val divididor= ds.map((line)=>{
      val splited= line.split(",")
      println(splited(0)+","+splited(1))
      (splited(0),splited(1))
    } )
    println(divididor.count())
    val flatted = ds.flatMap(_.toLowerCase.split(","))
    println(flatted.count())
    val tresprimeros=flatted.first(3).map((item)=>{
      println(item)
      item
    })
    println(flatted.collect()(1))
    val filtered = flatted.filter(_(1).toFloat< 35.0)
    println(filtered.count())
    // execute program
    //env.execute("Flink Scala API Skeleton")
  }


}
