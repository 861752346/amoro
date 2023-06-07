/*
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

package com.netease.arctic.optimizer.util;

import com.netease.arctic.optimizer.Optimizer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DefaultOptimizerSerializer {
  private DefaultOptimizerSerializer() {
  }

  public static Optimizer deserialize(byte[] bytes) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new ObjectInputStream(bais);
      Optimizer obj = (Optimizer) ois.readObject();

      bais.close();
      ois.close();
      return obj;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] serialize(Optimizer optimizer) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(optimizer);
      byte[] bs = baos.toByteArray();
      baos.close();
      oos.close();

      return bs;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}