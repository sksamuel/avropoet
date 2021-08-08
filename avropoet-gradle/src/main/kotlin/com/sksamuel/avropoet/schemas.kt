package com.sksamuel.avropoet

import org.apache.avro.Schema

fun Schema.isNullableUnion(): Boolean {
   return isUnion && types.size == 2 && types[0].type == Schema.Type.NULL && types[1].type != Schema.Type.NULL
}
