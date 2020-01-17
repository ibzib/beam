package org.apache.beam.sdk.extensions.sql.impl;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.Type.StructType;

/**
 * Immutable
 */
public abstract class QueryParameter<T> {
  private final String name;
  private final Type type;
  private final T value;

  public static class Type {
    private final TypeKind typeKind;
    private final Type arrayType;
    private final List<StructType> structTypes;

    public enum TypeKind {
      STRING,
      INT64,
      FLOAT64,
      BOOL,
      BYTES,
      TIMESTAMP,
      ARRAY,
      STRUCT,
      /** Represents the type of elements in an empty array. */
      VOID,
    }

    static class StructType {
      private final String name;
      private final Type type;

      StructType(String name, Type type) {
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

    private Type(TypeKind typeKind, @Nullable Type arrayType, @Nullable List<StructType> structTypes) {
      this.typeKind = typeKind;
      this.arrayType = arrayType;
      this.structTypes = structTypes;
    }

    private static Type createSimpleType(TypeKind typeKind) {
      return new Type(typeKind, null, null);
    }

    public static final Type STRING = createSimpleType(TypeKind.STRING);
    public static final Type INT64 = createSimpleType(TypeKind.INT64);
    public static final Type FLOAT64 = createSimpleType(TypeKind.FLOAT64);
    public static final Type BOOL = createSimpleType(TypeKind.BOOL);
    public static final Type BYTES = createSimpleType(TypeKind.BYTES);
    public static final Type TIMESTAMP = createSimpleType(TypeKind.TIMESTAMP);
    public static final Type VOID = createSimpleType(TypeKind.VOID);

    /**
     * TODO(ibzib) docstring
     */
    static Type createArrayType(Type arrayType) {
      return new Type(TypeKind.ARRAY, arrayType, null);
    }

    /**
     * TODO(ibzib) docstring
     */
    static Type createStructType(List<StructType> structTypes) {
      return new Type(TypeKind.STRUCT, null, structTypes);
    }

    public TypeKind getTypeKind() {
      return typeKind;
    }

    /**
     * @throws IllegalStateException when this type is not an array type.
     * @return The type of the values in the array, or VOID if the array is empty.
     * */
    public Type getArrayType() {
      // TODO(ibzib) reinforce this with generics for compile-time safety
      if (typeKind != TypeKind.ARRAY) {
        throw new IllegalStateException("Cannot get array subtype for non-array type " + typeKind);
      }
      return arrayType;
    }

    /**
     * @throws IllegalStateException when this type is not a struct type.
     * @return The respective types of the values in the struct.
     * */
    public List<StructType> getStructTypes() {
      if (typeKind != TypeKind.STRUCT) {
        throw new IllegalStateException(
            "Cannot get struct subtypes for non-struct type " + typeKind);
      }
      return structTypes;
    }
  }

  private static class ScalarParameter<T> extends QueryParameter<T> {
    ScalarParameter(String name, Type type, T value) {
      super(name, type, value);
    }
  }

  public static class StringParameter extends ScalarParameter<String> {
    public StringParameter(String name, String value) {
      super(name, Type.STRING, value);
    }
  }

  public static class TimestampParameter extends ScalarParameter<String> {
    public TimestampParameter(String name, String value) {
      super(name, Type.TIMESTAMP, value);
    }
  }

  public static class ArrayParameter<QP extends QueryParameter> extends QueryParameter<List<QP>> {
    public ArrayParameter(String name, List<QP> value) {
      super(
          name,
          Type.createArrayType(value.isEmpty() ? Type.VOID : value.get(0).getType()),
          value);
      // TODO(ibzib) validate
    }
  }

  public static class StructParameter extends QueryParameter<List<QueryParameter>> {
    public StructParameter(String name, List<StructType> memberTypes, List<QueryParameter> value) {
      super(name, Type.createStructType(memberTypes), value);
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

}

