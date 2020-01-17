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

/** TODO(ibzib) docstring */
public class ZetaSQLQueryParameterTranslator {

  /** TODO(ibzib) docstring */
  public static Map<String, Value> translate(List<QueryParameter> beamParameters, ParameterMode parameterMode) {
    final ImmutableMap.Builder<String, Value> zetaParameters = ImmutableMap.builder();
    for (QueryParameter beamParameter : beamParameters) {
      zetaParameters.put(beamParameter.getName(), queryParameterToValue(beamParameter));
    }
    // TODO(ibzib) if using positional parameters, we must give them names here.
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
        com.google.zetasql.Type type = null; // TODO(ibzib) convert qp to zeta type
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
                "Cannot translate query parameter %s with unknown type %s",
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
            String.format("cannot translate type %s", typeKind));
    }
  }
}
