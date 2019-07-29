package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;

@InterfaceStability.Evolving
public class ProxyRpcServerException extends RuntimeException {

  final RpcStatusProto status;
  final RpcErrorCodeProto code;
  final String errorClass;
  final String errorMessage;

  public ProxyRpcServerException(RpcStatusProto status,
                                 RpcErrorCodeProto code,
                                 String errorClass, String errorMessage) {
    this.status = status;
    this.code = code;
    this.errorClass = errorClass;
    this.errorMessage = errorMessage;
  }

  /**
   * get the rpc status corresponding to this exception
   */
  public RpcStatusProto getRpcStatusProto() {
    return status;
  }

  /**
   * get the detailed rpc status corresponding to this exception
   */
  public RpcErrorCodeProto getRpcErrorCodeProto() {
    return code;
  }

  @Override
  public String toString() {
    return "ProxyRpcServerException [" +
      "status=" + status + 
      ", code=" + code +
      ", errorClass=" + errorClass +
      ", errorMessage=" + errorMessage +
      ']';
  }
}