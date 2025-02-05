/*
 * Copyright © 2014-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.io;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for serialize/deserialize Schema object to/from json through {@link com.google.gson.Gson Gson}.
 * <p>
 *  Expected usage:
 *
 *  <pre>
 *    Schema schema = ...;
 *    Gson gson = new GsonBuilder()
 *                  .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
 *                  .create();
 *    String json = gson.toJson(schema);
 *
 *    Schema newSchema = gson.fromJson(json, Schema.class);
 *  </pre>
 * </p>
 */
public final class SchemaTypeAdapter extends TypeAdapter<Schema> {

  private static final String TYPE = "type";
  private static final String LOGICAL_TYPE = "logicalType";
  private static final String NAME = "name";
  private static final String SYMBOLS = "symbols";
  private static final String ITEMS = "items";
  private static final String KEYS = "keys";
  private static final String VALUES = "values";
  private static final String FIELDS = "fields";
  private static final String PRECISION = "precision";
  private static final String SCALE = "scale";

  @Override
  public void write(JsonWriter writer, Schema schema) throws IOException {
    if (schema == null) {
      writer.nullValue();
      return;
    }
    Set<String> definedTypes = new HashSet<>();
    write(writer, schema, definedTypes);
  }

  @Override
  public Schema read(JsonReader reader) throws IOException {
    return read(reader, new HashMap<>());
  }

  /**
   * Reads json value and convert it into {@link Schema} object.
   *
   * @param reader Source of json
   * @param definedTypes Set of named typed already encountered during the reading.
   * @return A {@link Schema} reflecting the json.
   * @throws IOException Any error during reading.
   */
  private Schema read(JsonReader reader, Map<String, Schema> definedTypes) throws IOException {
    JsonToken token = reader.peek();
    switch (token) {
      case NULL:
        // advance the value otherwise gson won't be able to get over the null value
        reader.nextNull();
        return null;
      case STRING: {
        // Avro allows records, enums, and fixed types to be named and referenced in other places
        String name = reader.nextString();
        /*
           schema is in the map if this is a recursive reference. For example,
           if we're looking at the inner 'node' record in the example below:
           {
             "type": "record",
             "name": "node",
             "fields": [{
               "name": "children",
               "type": [{
                 "type": "array",
                 "items": ["node", "null"]
               }, "null"]
             }, {
               "name": "data",
               "type": "int"
             }]
           }

           schema here may be an empty record or empty enum, but those will get fully resolved
           in the Schema constructor.
         */
        Schema definedType = definedTypes.get(name);
        if (definedType != null) {
          return definedType;
        }
        // otherwise, it is a simple type
        return Schema.of(Schema.Type.valueOf(name.toUpperCase()));
      }
      case BEGIN_ARRAY:
        // Union type
        return readUnion(reader, definedTypes);
      case BEGIN_OBJECT:
        return readObject(reader, definedTypes);
    }
    throw new IOException("Malformed schema input.");
  }

  /**
   * Read JSON object and return Schema corresponding to it.
   * @param reader JsonReader used to read the json object
   * @param definedTypes defined types already encountered during the reading.
   * @return Schema reflecting json
   * @throws IOException when error occurs during reading json
   */
  private Schema readObject(JsonReader reader, Map<String, Schema> definedTypes) throws IOException {
    reader.beginObject();
    // Type of the schema
    Schema.Type schemaType = null;
    // Logical Type of the schema
    Schema.LogicalType logicalType = null;
    // Name of the element
    String elementName = null;
    // Store enum values for ENUM type
    List<String> enumValues = new ArrayList<>();
    // Store schema for key and value for MAP type
    Schema keys = null;
    Schema values = null;
    // List of fields for RECORD type
    List<Schema.Field> fields = null;
    // List of items for ARRAY type
    Schema items = null;
    int precision = 0;
    int scale = 0;
    // Loop through current object and populate the fields as required
    // For ENUM type List of enumValues will be populated
    // For ARRAY type items will be populated
    // For MAP type keys and values will be populated
    // For RECORD type fields will be populated
    while (reader.hasNext()) {
      String name = reader.nextName();
      switch (name) {
        case LOGICAL_TYPE:
          logicalType = Schema.LogicalType.fromToken(reader.nextString());
          break;
        case PRECISION:
          precision = Integer.parseInt(reader.nextString());
          break;
        case SCALE:
          scale = Integer.parseInt(reader.nextString());
          break;
        case TYPE:
          schemaType = Schema.Type.valueOf(reader.nextString().toUpperCase());
          break;
        case NAME:
          elementName = reader.nextString();
          if (schemaType == Schema.Type.RECORD) {
            /*
              Put a record schema with empty fields in the map for the recursive references.
              For example, if we are looking at the outer 'node' reference in the example below, we
              add the record name in the knownRecords map, so that when we get to the inner 'node'
              reference, we know that its a record type and not a Schema.Type.
              {
                "type": "record",
                "name": "node",
                "fields": [{
                  "name": "children",
                  "type": [{
                    "type": "array",
                    "items": ["node", "null"]
                  }, "null"]
                },
                {
                  "name": "data",
                  "type": "int"
                }]
              }
              Full schema corresponding to this RECORD will be put in knownRecords once the fields in the
              RECORD are explored.
            */
            definedTypes.put(elementName, Schema.recordOf(elementName));
          } else if (schemaType == Schema.Type.ENUM) {
            definedTypes.put(elementName, Schema.enumWith(elementName, Collections.emptyList()));
          }
          break;
        case SYMBOLS:
          enumValues = readEnum(reader);
          definedTypes.put(elementName, Schema.enumWith(elementName, enumValues));
          break;
        case ITEMS:
          items = read(reader, definedTypes);
          break;
        case KEYS:
          keys = read(reader, definedTypes);
          break;
        case VALUES:
          values = read(reader, definedTypes);
          break;
        case FIELDS:
          fields = getFields(name, reader, definedTypes);
          definedTypes.put(elementName, Schema.recordOf(elementName, fields));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.endObject();
    if (schemaType == null) {
      throw new IllegalStateException("Schema type cannot be null.");
    }

    if (logicalType != null) {
      if (logicalType == Schema.LogicalType.DECIMAL) {
        try {
          return Schema.decimalOf(precision, scale);
        } catch (IllegalArgumentException e) {
          throw new IOException("Decimal type must contain a positive precision value.");
        }
      }

      return Schema.of(logicalType);
    }

    Schema schema;
    switch (schemaType) {
      case ARRAY:
        schema = Schema.arrayOf(items);
        break;
      case ENUM:
        schema = Schema.enumWith(elementName, enumValues);
        break;
      case MAP:
        // avro schema doesn't define a key type
        // for compatibility, assume key is nullable string if none is given
        if (keys == null) {
          keys = Schema.nullableOf(Schema.of(Schema.Type.STRING));
        }
        schema = Schema.mapOf(keys, values);
        break;
      case RECORD:
        schema = Schema.recordOf(elementName, fields);
        break;
      default:
        schema = Schema.of(schemaType);
        break;
    }
    return schema;
  }

  /**
   * Constructs {@link Schema.Type#UNION UNION} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @param definedTypes Map of defined type names and associated schema already encountered during the reading
   * @return A {@link Schema} of type {@link Schema.Type#UNION UNION}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readUnion(JsonReader reader, Map<String, Schema> definedTypes) throws IOException {
    List<Schema> unionSchemas = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      unionSchemas.add(read(reader, definedTypes));
    }
    reader.endArray();
    return Schema.unionOf(unionSchemas);
  }

  /**
   * Returns the {@link List} of enum values from the json input.
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return a list of enum values
   * @throws IOException When fails to parse the input json.
   */
  private List<String> readEnum(JsonReader reader) throws IOException {
    List<String> enumValues = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      enumValues.add(reader.nextString());
    }
    reader.endArray();
    return enumValues;
  }

  /**
   * Get the list of {@link Schema.Field} associated with current RECORD.
   * @param recordName the name of the RECORD for which fields to be returned
   * @param reader the reader to read the record
   * @param definedTypes defined type names already encountered during the reading
   * @return the list of fields associated with the current record
   * @throws IOException when error occurs during reading the json
   */
  private List<Schema.Field> getFields(String recordName, JsonReader reader, Map<String, Schema> definedTypes)
    throws IOException {
    definedTypes.put(recordName, Schema.recordOf(recordName));
    List<Schema.Field> fieldBuilder = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      reader.beginObject();
      String fieldName = null;
      Schema innerSchema = null;

      while (reader.hasNext()) {
        String name = reader.nextName();
        switch(name) {
          case NAME:
            fieldName = reader.nextString();
            break;
          case TYPE:
            innerSchema = read(reader, definedTypes);
            break;
          default:
            reader.skipValue();
        }
      }
      fieldBuilder.add(Schema.Field.of(fieldName, innerSchema));
      reader.endObject();
    }
    reader.endArray();
    return fieldBuilder;
  }

  /**
   * Writes the given {@link Schema} into json.
   *
   * @param writer A {@link JsonWriter} for emitting json.
   * @param schema The {@link Schema} object to encode to json.
   * @param definedTypes Set of defined type names that has already been encoded.
   * @return The same {@link JsonWriter} as the one passed in.
   * @throws IOException When fails to encode the schema into json.
   */
  private JsonWriter write(JsonWriter writer, Schema schema, Set<String> definedTypes) throws IOException {
    if (schema.getLogicalType() != null) {
      writer.beginObject().name(TYPE).value(schema.getType().name().toLowerCase());
      writer.name(LOGICAL_TYPE).value(schema.getLogicalType().getToken());
      if (schema.getLogicalType() == Schema.LogicalType.DECIMAL) {
        writer.name("precision").value(schema.getPrecision());
        writer.name("scale").value(schema.getScale());
      }
      writer.endObject();
      return writer;
    }

    // Simple type, just emit the type name as a string
    if (schema.getType().isSimpleType()) {
      return writer.value(schema.getType().name().toLowerCase());
    }

    // Union type is an array of schemas
    if (schema.getType() == Schema.Type.UNION) {
      writer.beginArray();
      for (Schema unionSchema : schema.getUnionSchemas()) {
        write(writer, unionSchema, definedTypes);
      }
      return writer.endArray();
    }

    // If it is a record or enum that refers to a previously defined schema, just emit the name of it
    String typeName = null;
    if (schema.getType() == Schema.Type.RECORD) {
      typeName = schema.getRecordName();
    } else if (schema.getType() == Schema.Type.ENUM) {
      typeName = schema.getEnumName();
    }
    if (definedTypes.contains(typeName)) {
      return writer.value(typeName);
    }
    // Complex types, represented as an object with "type" property carrying the type name
    writer.beginObject().name(TYPE).value(schema.getType().name().toLowerCase());
    switch (schema.getType()) {
      case ENUM:
        // Emits all enum values as an array, keyed by "symbols"
        String enumName = schema.getEnumName();
        if (enumName != null) {
          definedTypes.add(enumName);
          writer.name(NAME).value(enumName);
        }
        writer.name(SYMBOLS).beginArray();
        for (String enumValue : schema.getEnumValues()) {
          writer.value(enumValue);
        }
        writer.endArray();
        break;

      case ARRAY:
        // Emits the schema of the array component type, keyed by "items"
        write(writer.name(ITEMS), schema.getComponentSchema(), definedTypes);
        break;

      case MAP:
        // Emits schema of both key and value types, keyed by "keys" and "values" respectively
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        write(writer.name(KEYS), mapSchema.getKey(), definedTypes);
        write(writer.name(VALUES), mapSchema.getValue(), definedTypes);
        break;

      case RECORD:
        // Emits the name of record, keyed by "name"
        definedTypes.add(schema.getRecordName());
        writer.name(NAME).value(schema.getRecordName())
              .name(FIELDS).beginArray();
        // Each field is an object, with field name keyed by "name" and field schema keyed by "type"
        for (Schema.Field field : schema.getFields()) {
          writer.beginObject().name(NAME).value(field.getName());
          write(writer.name(TYPE), field.getSchema(), definedTypes);
          writer.endObject();
        }
        writer.endArray();
        break;
    }
    writer.endObject();

    return writer;
  }
}
