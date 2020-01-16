package org.apache.beam.sdk.extensions.sql.impl;

import java.util.List;
import java.util.Map;

public class QueryParameter {
  String name;
  QueryParameterType parameterType;
  QueryParameterValue parameterValue;

  class QueryParameterType {
    String type;
    // Null if this is not an array type.
    QueryParameterType arrayType;
    // TODO(ibzib) empty? Null if this is not a struct type.
    List<QueryParameterType> structTypes;
  }

  class QueryParameterValue {
    String value;
    // Empty if this is not an array value. TODO(ibzib) empty or null?
    List<QueryParameterValue> arrayValues;
    // Empty if this is not a struct value. TODO(ibzib) empty or null?
    Map<String, QueryParameterValue> structValues;
  }
}

