package org.apache.pinot.core.operator.filter.predicate;

import com.google.common.collect.Lists;
import java.math.BigDecimal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MultiValueVisitor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InPredicateEvaluatorFactoryTest {

  MultiValueVisitor<Integer> createValueLengthVisitor() {
    return new MultiValueVisitor<Integer>() {
      @Override
      public Integer visitInt(int[] value) {
        return value.length;
      }

      @Override
      public Integer visitLong(long[] value) {
        return value.length;
      }

      @Override
      public Integer visitFloat(float[] value) {
        return value.length;
      }

      @Override
      public Integer visitDouble(double[] value) {
        return value.length;
      }

      @Override
      public Integer visitBigDecimal(BigDecimal[] value) {
        return value.length;
      }

      @Override
      public Integer visitBoolean(boolean[] value) {
        return value.length;
      }

      @Override
      public Integer visitTimestamp(long[] value) {
        return value.length;
      }

      @Override
      public Integer visitString(String[] value) {
        return value.length;
      }

      @Override
      public Integer visitJson(String[] value) {
        return value.length;
      }

      @Override
      public Integer visitBytes(byte[][] value) {
        return value.length;
      }
    };
  }

  @Test
  void canBeVisited() {
    // Given a visitor
    MultiValueVisitor<Integer> valueLengthVisitor = Mockito.spy(createValueLengthVisitor());

    // When int predicate is used
    InPredicate predicate = new InPredicate(ExpressionContext.forIdentifier("ident"), Lists.newArrayList("1", "2"));

    InPredicateEvaluatorFactory.InRawPredicateEvaluator intEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.INT);

    // Only the int[] method is called
    int length = intEvaluator.accept(valueLengthVisitor);
    Assert.assertEquals(length, 2);
    Mockito.verify(valueLengthVisitor).visitInt(new int[] {2, 1});
    Mockito.verifyNoMoreInteractions(valueLengthVisitor);

    // And given a string predicate
    InPredicateEvaluatorFactory.InRawPredicateEvaluator strEvaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, FieldSpec.DataType.STRING);

    // Only the string[] method is called
    length = strEvaluator.accept(valueLengthVisitor);
    Assert.assertEquals(length, 2);
    Mockito.verify(valueLengthVisitor).visitString(new String[] {"2", "1"});
    Mockito.verifyNoMoreInteractions(valueLengthVisitor);
  }
}