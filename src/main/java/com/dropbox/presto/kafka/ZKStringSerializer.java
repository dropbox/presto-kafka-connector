/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dropbox.presto.kafka;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;


public class ZKStringSerializer implements ZkSerializer {
  public byte[] serialize(java.lang.Object o)
      throws org.I0Itec.zkclient.exception.ZkMarshallingError {
    try {
      return ((String) o).getBytes("UTF-8");
    } catch (UnsupportedEncodingException ex) {
      return null;
    }
  }

  public java.lang.Object deserialize(byte[] bytes)
      throws org.I0Itec.zkclient.exception.ZkMarshallingError {
    if (bytes == null)
      return null;
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      return null;
    }
  }
}