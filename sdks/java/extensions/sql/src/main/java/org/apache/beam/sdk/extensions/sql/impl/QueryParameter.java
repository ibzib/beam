package org.apache.beam.sdk.extensions.sql.impl;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.Type.ArrayType;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.Type.StructMemberType;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.Type.StructType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.interpreter.Scalar;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link QueryParameter} is an abstraction layer for providing runtime parameters to queries written in different SQL dialects. Before use, parameters must be translated into the native representation of the SQL implementation. The interpretation of some parameter values may vary according to the SQL dialect.
 *
 * A parameter is an immutable tuple of name, type, and value. Note that the name may be left empty depending on the {@link ParameterMode} used by the application.
 *
 * Query parameters are statically type checked except for {@link StructParameter}. Structs will be
 * validated at runtime.
 *
 * @param <T> The native Java type for the value of this parameter.
 */
public abstract class QueryParameter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(QueryParameter.class);
  private final String name;
  private final Type type;
  private final T value;

  /** Parameters have different usage semantics depending on the parameter mode specified. Note that some parameter modes might not be supported by all backends. */
  public enum ParameterMode {
    /** Parameter values are resolved according to their names. Each parameter must have a name. Parameters may be listed in any arbitrary order. Like variables, parameters may be reused any number of times. */
    NAMED,
    /** Parameters are unnamed and resolved according to the order they are listed.
     * Each parameter is used only once. */
    POSITIONAL,
  }

  /** Enumeration of the supported parameter types. */
  public enum TypeKind {
    STRING,
    INT64,
    FLOAT64,
    BOOL,
    BYTES,
    TIMESTAMP,
    ARRAY,
    STRUCT,
  }

  /** TODO(ibzib) javadoc */
  public static class Type {
    private final TypeKind typeKind;

    /** A member of a {@link StructType}. */
    public static class StructMemberType {
      private final String name;
      private final Type type;

      StructMemberType(String name, Type type) {
        this.name = name;
        this.type = type;
      }

      public String getName() {
        return name;
      }

      public Type getType() {
        return type;
      }
    }

    public static class StructType extends Type {
      private final List<StructMemberType> memberTypes;

      StructType(List<StructMemberType> memberTypes) {
        super(TypeKind.STRUCT);
        this.memberTypes = memberTypes;
      }

      public List<StructMemberType> getMemberTypes() {
        return memberTypes;
      }
    }

    public static class ArrayType extends Type {
      private final Type elementType;

      ArrayType(Type elementType) {
        super(TypeKind.ARRAY);
        this.elementType = elementType;
      }

      public Type getElementType() {
        return elementType;
      }
    }

    private Type(TypeKind typeKind) {
      this.typeKind = typeKind;
    }

    public static final Type STRING = new Type(TypeKind.STRING);
    public static final Type INT64 = new Type(TypeKind.INT64);
    public static final Type FLOAT64 = new Type(TypeKind.FLOAT64);
    public static final Type BOOL = new Type(TypeKind.BOOL);
    public static final Type BYTES = new Type(TypeKind.BYTES);
    public static final Type TIMESTAMP = new Type(TypeKind.TIMESTAMP);

    public TypeKind getTypeKind() {
      return typeKind;
    }
  }

  private static class ScalarParameter<T> extends QueryParameter<T> {
    ScalarParameter(String name, Type type, T value) {
      super(name, type, value);
    }
    ScalarParameter(Type type, T value) {
      super("", type, value);
    }
  }

  public static class StringParameter extends ScalarParameter<String> {
    public StringParameter(String name, String value) {
      super(name, Type.STRING, value);
    }
    public StringParameter(String value) {
      super(Type.STRING, value);
    }
  }

  public static class TimestampParameter extends ScalarParameter<String> {
    public TimestampParameter(String name, String value) {
      super(name, Type.TIMESTAMP, value);
    }
    public TimestampParameter(String value) {
      super(Type.TIMESTAMP, value);
    }
  }

  /** A parameter containing a list of other parameters. All elements must have the same type. */
  public static class ArrayParameter<QP extends QueryParameter> extends QueryParameter<List<QP>> {

    public ArrayParameter(String name, Type elementType, List<QP> elements) {
      super(name, new ArrayType(elementType), elements);
      // TODO(ibzib) type check
    }

    /** Convenience method for constructing an {@link ArrayParameter} from scalar values. */
    @SafeVarargs
    public static <T> ArrayParameter<ScalarParameter<T>> of(String name, Type elementType, T... elementValues) {
      ArrayList<ScalarParameter<T>> elementParameters = new ArrayList<>();
      for (T elementValue : elementValues) {
        elementParameters.add(new ScalarParameter<>(elementType, elementValue));
      }
      return new ArrayParameter<>(name, elementType, elementParameters);
    }
  }

  public static class StructParameter extends QueryParameter<List<QueryParameter>> {
    public StructParameter(String name, List<StructMemberType> memberTypes, List<QueryParameter> value) {
      super(name, new StructType(memberTypes), value);
      // TODO(ibzib) validate
    }
  }

  private QueryParameter(String name, Type type, T value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public T getValue() {
    return value;
  }

  public void validate() {
    // TODO(ibzib) typecheck
  }

  /**
   * Checks that query parameters are valid for the given parameter mode.
   * @throws IllegalArgumentException if the parameters are not valid.
   * */
  public static void validate(List<QueryParameter> queryParameters, ParameterMode parameterMode) {
    for (QueryParameter parameter : queryParameters) {
    if (parameterMode == ParameterMode.NAMED) {
        Preconditions.checkArgument(!parameter.getName().isEmpty(), String.format("Cannot use unnamed parameter of type %s in named parameter mode.", parameter.getType().getTypeKind()));
      } else if (parameterMode == ParameterMode.POSITIONAL) {
      if (!parameter.getName().isEmpty()) {
        LOG.warn("Warning: attempting to use named parameter {} in positional parameter mode.", parameter.getName());
      }
    }
    }
  }

}

