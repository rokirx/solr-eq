/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

import org.apache.lucene.util.BytesRef;

/**
 * Stores information about how to sort documents by terms in an individual
 * field.  Fields must be indexed in order to sort by them.
 *
 * <p>Created: Feb 11, 2004 1:25:29 PM
 *
 * @since   lucene 1.4
 * @see Sort
 */
public class EqSortField extends SortField {
  public static final EqSortField FIELD_SCORE;
  private String eqField;
  private Type eqType;  // defaults to determining type dynamically
  boolean reverse = false;  // defaults to natural order


  static {
    FIELD_SCORE = new EqSortField(null, SortField.Type.SCORE);
//    FIELD_DOC = new SortField((String)null, SortField.Type.DOC);
//    STRING_FIRST = new Object() {
//      public String toString() {
//        return "SortField.STRING_FIRST";
//      }
//    };
//    STRING_LAST = new Object() {
//      public String toString() {
//        return "SortField.STRING_LAST";
//      }
//    };
  }

  // Used for CUSTOM sort
  private FieldComparatorSource comparatorSource;

  // Used for 'sortMissingFirst/Last'
  protected Object missingValue = null;

  /** Creates a sort by terms in the given field with the type of term
   * values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   */
  public EqSortField(String field, Type type) {
    super(field, type);
  }

  public EqSortField(SortField f){
    super(f.getField(), f.getType(), f.getReverse());
    eqField = f.getField();
    eqType = f.getType();
  }

  /** Creates a sort, possibly in reverse, by terms in the given field with the
   * type of term values explicitly given.
   * @param field  Name of field to sort by.  Can be <code>null</code> if
   *               <code>type</code> is SCORE or DOC.
   * @param type   Type of values in the terms.
   * @param reverse True if natural order should be reversed.
   */
  public EqSortField(String field, Type type, boolean reverse) {
    super(field, type, reverse);
  }

  /** Pass this to {@link #setMissingValue} to have missing
   *  string values sort first. */
  public final static Object STRING_FIRST = new Object() {
      @Override
      public String toString() {
        return "SortField.STRING_FIRST";
      }
    };
  
  /** Pass this to {@link #setMissingValue} to have missing
   *  string values sort last. */
  public final static Object STRING_LAST = new Object() {
      @Override
      public String toString() {
        return "SortField.STRING_LAST";
      }
    };

  /** Creates a sort with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   */
  public EqSortField(String field, FieldComparatorSource comparator) {
    super(field, comparator);
  }

  /** Creates a sort, possibly in reverse, with a custom comparison function.
   * @param field Name of field to sort by; cannot be <code>null</code>.
   * @param comparator Returns a comparator for sorting hits.
   * @param reverse True if natural order should be reversed.
   */
  public EqSortField(String field, FieldComparatorSource comparator, boolean reverse) {
    super(field, comparator, reverse);
  }

  /** Returns the {@link FieldComparatorSource} used for
   * custom sorting
   */
  public FieldComparatorSource getComparatorSource() {
    return comparatorSource;
  }

  /** Returns true if <code>o</code> is equal to this.  If a
   *  {@link FieldComparatorSource} was provided, it must properly
   *  implement equals (unless a singleton is always used). */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EqSortField)) return false;
    return super.equals(o);
  }

  private Comparator<BytesRef> bytesComparator = Comparator.naturalOrder();

  public void setBytesComparator(Comparator<BytesRef> b) {
    bytesComparator = b;
  }

  public Comparator<BytesRef> getBytesComparator() {
    return bytesComparator;
  }

  /** Returns the {@link FieldComparator} to use for
   * sorting.
   *
   * @lucene.experimental
   *
   * @param numHits number of top hits the queue will store
   * @param sortPos position of this SortField within {@link
   *   Sort}.  The comparator is primary if sortPos==0,
   *   secondary if sortPos==1, etc.  Some comparators can
   *   optimize themselves when they are the primary sort.
   * @return {@link FieldComparator} to use when sorting
   */
  public FieldComparator<?> getEqComparator(final int numHits, final int sortPos, final int sqidx) {

    switch (eqType) {
    case SCORE:
      return new EqFieldComparator.RelevanceComparator(numHits);

    case DOC:
      return new EqFieldComparator.DocComparator(numHits);

    case INT:
      return new EqFieldComparator.IntComparator(numHits, eqField, (Integer) missingValue, sqidx);

    case FLOAT:
      return new EqFieldComparator.FloatComparator(numHits, eqField, (Float) missingValue, sqidx);

    case LONG:
      return new EqFieldComparator.LongComparator(numHits, eqField, (Long) missingValue, sqidx);

    case DOUBLE:
      return new EqFieldComparator.DoubleComparator(numHits, eqField, (Double) missingValue, sqidx);

    case CUSTOM:
      assert comparatorSource != null;
      return comparatorSource.newComparator(eqField, numHits, sortPos, reverse);

    case STRING:
      return new EqFieldComparator.TermOrdValComparator(numHits, eqField, missingValue == STRING_LAST, sqidx);

    case STRING_VAL:
      return new EqFieldComparator.TermValComparator(numHits, eqField, missingValue == STRING_LAST, sqidx);

    case REWRITEABLE:
      throw new IllegalStateException("SortField needs to be rewritten through Sort.rewrite(..) and SortField.rewrite(..)");
        
    default:
      throw new IllegalStateException("Illegal sort type: " + eqType);
    }
  }

  /**
   * Rewrites this SortField, returning a new SortField if a change is made.
   * Subclasses should override this define their rewriting behavior when this
   * SortField is of type {@link SortField.Type#REWRITEABLE}
   *
   * @param searcher IndexSearcher to use during rewriting
   * @return New rewritten SortField, or {@code this} if nothing has changed.
   * @throws IOException Can be thrown by the rewriting
   * @lucene.experimental
   */
  public SortField rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }
}
