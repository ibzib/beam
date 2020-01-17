package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.Value;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.StringParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.TimestampParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.Type;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** TODO(ibzib) docstring */
public class ZetaSQLQueryParameterTranslator {

  /** TODO(ibzib) docstring */
  public static Map<String, Value> translate(List<QueryParameter> beamParameters) {
    final ImmutableMap.Builder<String, Value> zetaParameters = ImmutableMap.builder();
    for (QueryParameter beamParameter : beamParameters) {
      zetaParameters.put(beamParameter.getName(), queryParameterToValue(beamParameter));
    }
    return zetaParameters.build();
  }

  @VisibleForTesting
  static Value queryParameterToValue(QueryParameter queryParameter) {
    final Type.TypeKind typeKind = queryParameter.getType().getTypeKind();
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
        return null;
      case STRUCT:
        return null;
      case VOID:
        // fall through
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate query parameter %s with unknown type %s",
                queryParameter.getName(),
                typeKind));
    }
  }
}
