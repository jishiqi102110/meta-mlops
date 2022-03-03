package com.meta.entity

//scalastyle:off
/**
 * @author weitaoliang 
 * @version V1.0
 * */

object  SerializeTypeEnum extends Enumeration {
     type SerializeTypeEnum=Value
     val BYTES:Value = Value("bytes")
     val PROTO:Value = Value("proto")
}
