package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.Value;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.StringParameter;
import org.apache.beam.sdk.extensions.sql.impl.QueryParameter.TimestampParameter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ZetaSQLQueryParameterTranslator}. */
@RunWith(JUnit4.class)
public class ZetaSQLQueryParameterTranslatorTest {
  @Test
  public void testStringParameter() {
    String str = "23";
    QueryParameter param = new StringParameter("", str);
    Value value = ZetaSQLQueryParameterTranslator.queryParameterToValue(param);
    Assert.assertEquals(str, value.getStringValue());
  }

  // @Test
  // public void testIntParameter() {
  //   String str = "23";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isInt64());
  // }
  //
  // @Test
  // public void testBoolParameter() {
  //   String str = "FALSE";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isBool());
  // }
  //
  // @Test
  // public void testFloat64Max() {
  //   String str = "1.7976931348623157e+308";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isDouble());
  //   Assert.assertEquals(1.7976931348623157E308, value.getDoubleValue(), 0);
  // }
  //
  // @Test
  // public void testInt64Max() {
  //   String str = "9223372036854775807";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isInt64());
  // }
  //
  // @Test
  // public void testInt64MaxPlusOne() {
  //   String str = "9223372036854775808";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isDouble()); // actually uint64
  // }
  //
  // @Test
  // public void testStructParameter() {
  //   String str = "STRUCT<int64, date>(5, \"2011-05-05\")";
  //   // TODO(ibzib) implement me
  //   Value value;
  //   Assert.assertTrue(value.getType().isStruct());
  // }

  @Test
  public void testTimestampParameterWithOffset() {
    String timestampStr = "2018-12-10 10:38:59-1000";
    QueryParameter param = new TimestampParameter("", timestampStr);
    Value value = ZetaSQLQueryParameterTranslator.queryParameterToValue(param);
    Assert.assertEquals(1544474339000000L, value.getTimestampUnixMicros());
  }

}
