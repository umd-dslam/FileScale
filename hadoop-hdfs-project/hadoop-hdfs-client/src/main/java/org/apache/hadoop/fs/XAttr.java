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
package org.apache.hadoop.fs;

import java.util.Arrays;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import java.nio.charset.StandardCharsets;
import java.io.UnsupportedEncodingException;

/**
 * XAttr is the POSIX Extended Attribute model similar to that found in
 * traditional Operating Systems.  Extended Attributes consist of one
 * or more name/value pairs associated with a file or directory. Five
 * namespaces are defined: user, trusted, security, system and raw.
 *   1) USER namespace attributes may be used by any user to store
 *   arbitrary information. Access permissions in this namespace are
 *   defined by a file directory's permission bits. For sticky directories,
 *   only the owner and privileged user can write attributes.
 * <br>
 *   2) TRUSTED namespace attributes are only visible and accessible to
 *   privileged users. This namespace is available from both user space
 *   (filesystem API) and fs kernel.
 * <br>
 *   3) SYSTEM namespace attributes are used by the fs kernel to store
 *   system objects.  This namespace is only available in the fs
 *   kernel. It is not visible to users.
 * <br>
 *   4) SECURITY namespace attributes are used by the fs kernel for
 *   security features. It is not visible to users.
 * <br>
 *   5) RAW namespace attributes are used for internal system attributes that
 *   sometimes need to be exposed. Like SYSTEM namespace attributes they are
 *   not visible to the user except when getXAttr/getXAttrs is called on a file
 *   or directory in the /.reserved/raw HDFS directory hierarchy.  These
 *   attributes can only be accessed by the superuser.
 * <p>
 * @see <a href="http://en.wikipedia.org/wiki/Extended_file_attributes">
 * http://en.wikipedia.org/wiki/Extended_file_attributes</a>
 *
 */
@InterfaceAudience.Private
public class XAttr {

  public enum NameSpace {
    USER,
    TRUSTED,
    SECURITY,
    SYSTEM,
    RAW
  }

  private final NameSpace ns;
  private final String name;
  private final byte[] value;

  public static class Builder {
    private NameSpace ns = NameSpace.USER;
    private String name;
    private byte[] value;

    public Builder setNameSpace(NameSpace ns) {
      this.ns = ns;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setValue(byte[] value) {
      this.value = value;
      return this;
    }

    public XAttr build() {
      return new XAttr(ns, name, value);
    }
  }

  // Using the charset canonical name for String/byte[] conversions is much
  // more efficient due to use of cached encoders/decoders.
  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  private XAttr(NameSpace ns, String name, byte[] value) {
    this.ns = ns;
    this.name = name;	
    this.value = value;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  private static byte[] string2Bytes(String str) {
    try {
      return str.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  private static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  private static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  public NameSpace getNameSpace() {
    return ns;
  }

  public String getName() {
    return name;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public static int hashCode() {
    return new HashCodeBuilder(811, 67)
        .append(getName())
        .append(getNameSpace())
        .append(getValue())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(getNameSpace(), rhs.getNameSpace())
        .append(getName(), rhs.getName())
        .append(getValue(), rhs.getValue())
        .isEquals();
  }

  /**
   * Similar to {@link #equals(Object)}, except ignores the XAttr value.
   *
   * @param obj to compare equality
   * @return if the XAttrs are equal, ignoring the XAttr value
   */
  public boolean equalsIgnoreValue(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    XAttr rhs = (XAttr) obj;
    return new EqualsBuilder()
        .append(getNameSpace(), rhs.getNameSpace())
        .append(getName(), rhs.getName())
        .isEquals();
  }

  @Override
  public String toString() {
    return "XAttr [ns=" + getNameSpace() + ", name=" + getName() + ", value="
        + Arrays.toString(getValue()) + "]";
  }
}
