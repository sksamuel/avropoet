# avropoet

![master](https://github.com/sksamuel/avropoet/workflows/main/badge.svg)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avropoet/avropoet-gradle.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Choplite)

This project generates Kotlin data classes from Avro definitions, along with encoders and decoders for moving
between `GenericRecord`s and the data classes.

## Getting started

Add this plugin to your gradle build:

```kotlin
plugins {
   kotlin("com.sksamuel.avropoet")
}
```

Create a main resource directory named `/src/main/avro`:

```kotlin
sourceSets {
   main {
      resources {
         setSrcDirs(listOf("src/main/resources", "src/main/avro"))
      }
   }
}
```

Then place your avro definitions into this `/src/main/avro` directory.

Then whenever the `avropoet/generateSources` task is executed, each of the records defined in the avro definitions will
be generated as a data class.

Note: All .json or .avdl files are included, even those in nested folders. This allows you to layout your files however
you want for readability.

### Example

Take this sample avro file:

```json
{
   "type": "record",
   "namespace": "com.sksamuel.foo",
   "name": "UserProfile",
   "fields": [
      {
         "name": "created",
         "type": {
            "type": "long",
            "logicalType": "timestamp-millis"
         }
      },
      {
         "name": "userId",
         "type": "long"
      },
      {
         "name": "username",
         "type": "string"
      }
   ]
}
```

This data class would be generated:

```kotlin
package com.sksamuel.foo

public data class UserProfile(
   val created: Timestamp,
   val userId: Long,
   val username: String
)
```

Note: The public keywords are added as an artifact of the way kotlin poet generates kotlin source.

## Schemas

A companion object with a val named `schema` is added to each record. This val returns an Avro `Schema` instance that
contains the parsed instance of the schema definition.

Eg, in the previous example, we could do `UserProfile.schema`

## Encoders

An extension method is added for each record generated.

This extension method is called `encode()` and returns a `GenericRecord` populated from the values of the data class.

Eg, `val record = UserProfile.encode()`

## Decoders

Each generated record has a companion object function called decode, which accepts a `GenericRecord` and returns a
decoded type. This is the inverse of the encoding function.

Eg, `val profile = UserProfile.decode(record)`

## Type names and packages

The fully qualified name of the Kotlin data classes is taken from the avro definition and not the file structure. For
example, take the following avro definition:

```json
{
   "type": "record",
   "namespace": "com.sksamuel.foo",
   "name": "MyGreatEvent",
   "fields": [
      ...
   ]
}
```

This would generate a data class called `MyGreatEvent` in the package `com.sksamuel.foo`.

## Referencing types

Avro records can refer to other records by name (instead of defining inline). This is useful if you have a shared record
type. For this to work the Avro parser needs to have parsed the referenced type before it parses the main record.

In order to make this easy, avropoet will process any files located in `/shared` before any other files. Place the
dependent files there, and they will be in the buffer of the Avro parser when it processes your main files.

## Supported logical types

Avropoet currently supports the following logical types:

* Timestamp-millis

## License

```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2021 Stephen Samuel

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
