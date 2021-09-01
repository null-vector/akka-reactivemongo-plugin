package org.nullvector

case class AdapterKey(payloadType: Class[_]) {

  override def hashCode(): Int = payloadType.getPackage.hashCode()

  override def equals(obj: Any): Boolean =
    payloadType.isAssignableFrom(obj.asInstanceOf[AdapterKey].payloadType)
}
