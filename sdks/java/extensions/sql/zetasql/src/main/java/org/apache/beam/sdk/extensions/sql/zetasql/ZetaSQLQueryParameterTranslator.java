package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.ArrayType;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.ArrayParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.ParameterMode;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.StringParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.TimestampParameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Contains methods for translating Beam query parameters into ZetaSQL query parameters. */
public class ZetaSQLQueryParameterTranslator {

  /** Translates Beam query parameters into ZetaSQL query parameters. */
  static Map<String, Value> translate(List<QueryParameter> beamParameters, ParameterMode parameterMode) {
    final ImmutableMap.Builder<String, Value> zetaParameters = ImmutableMap.builder();
    int i = 1;
    for (QueryParameter beamParameter : beamParameters) {
      String key;
      if (parameterMode == ParameterMode.POSITIONAL) {
        // Positional parameters must be keyed in ZetaSQL, even though the keys are ultimately discarded.
        key = String.format("parameter%d", i);
      } else {
        key = beamParameter.getName();
      }
      zetaParameters.put(key, queryParameterToValue(beamParameter));
      i++;
    }
    return zetaParameters.build();
  }

  @VisibleForTesting
  static Value queryParameterToValue(QueryParameter queryParameter) {
    final QueryParameter.TypeKind typeKind = queryParameter.getType().getTypeKind();
    switch (typeKind) {
      case STRING:
        return Value.createStringValue(((StringParameter)queryParameter).getValue());
      case INT64:
        return null;
      case FLOAT64:
        return null;
      case BOOL:
        return null;
      case BYTES:
        return null;
      case TIMESTAMP:
        return DateTimeUtils.parseTimestampWithTZToValue(((TimestampParameter)queryParameter).getValue());
      case ARRAY:
        ArrayParameter<? extends QueryParameter> arrayParameter = (ArrayParameter<?>) queryParameter;
        // darn
        com.google.zetasql.Type type = getZetaSQLType(queryParameter.getType());
        ArrayList<Value> values = new ArrayList<>();
        for (QueryParameter element : arrayParameter.getValue()) {
          values.add(queryParameterToValue(element));
        }
        return Value.createArrayValue(TypeFactory.createArrayType(type), values);
      case STRUCT:
        return null;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate query parameter %s with unknown type %s.",
                queryParameter.getName(),
                typeKind));
    }
  }

  @VisibleForTesting
  static com.google.zetasql.Type getZetaSQLType(QueryParameter.Type inputType) {
    QueryParameter.TypeKind typeKind = inputType.getTypeKind();
    switch (typeKind) {
      case STRING:
        return TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
      case INT64:
        return TypeFactory.createSimpleType(TypeKind.TYPE_INT64);
      case FLOAT64:
        return TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT);
      case BOOL:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BOOL);
      case BYTES:
        return TypeFactory.createSimpleType(TypeKind.TYPE_BYTES);
      case TIMESTAMP:
        return TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP);
      case ARRAY:
        return null;
      case STRUCT:
        return null;
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot translate type %s.", typeKind));
    }
  }
}
