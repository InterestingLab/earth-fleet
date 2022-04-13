/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.webhook.source

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.apache.spark.sql.execution.streaming.MemoryStream

class HttpPushServlet(stream: MemoryStream[HttpData]) extends HttpServlet {

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val resBody = Source.fromInputStream(req.getInputStream).mkString
    val timestamp = new java.sql.Timestamp(System.currentTimeMillis())
    stream.addData(HttpData(resBody, timestamp))

    resp.setContentType("application/json;charset=utf-8")
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter.write("""{"success": true}""")
  }

}
