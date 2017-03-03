/**
 * Copyright (C) 2017-2017 - All rights reserved.
 * This file is part of the telepathdb project which is released under the GPLv3 license.
 * See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
 * You may use, distribute and modify this code under the terms of the GPLv3 license.
 */

package com.telepathdb.memorymanager.spliterator;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import static java.util.Spliterators.spliterator;

public abstract class FixedBatchSpliteratorBase<T> implements Spliterator<T> {
  private final int batchSize;
  private final int characteristics;
  private long est;

  public FixedBatchSpliteratorBase(int characteristics, int batchSize, long est) {
    this.characteristics = characteristics | SUBSIZED;
    this.batchSize = batchSize;
    this.est = est;
  }

  public FixedBatchSpliteratorBase(int characteristics, int batchSize) {
    this(characteristics, batchSize, Long.MAX_VALUE);
  }

  public FixedBatchSpliteratorBase(int characteristics) {
    this(characteristics, 128, Long.MAX_VALUE);
  }

  public FixedBatchSpliteratorBase() {
    this(IMMUTABLE | ORDERED | NONNULL);
  }

  @Override
  public Spliterator<T> trySplit() {
    final HoldingConsumer<T> holder = new HoldingConsumer<>();
    if (!tryAdvance(holder)) return null;
    final Object[] a = new Object[batchSize];
    int j = 0;
    do a[j] = holder.value; while (++j < batchSize && tryAdvance(holder));
    if (est != Long.MAX_VALUE) est -= j;
    return spliterator(a, 0, j, characteristics() | SIZED);
  }

  @Override
  public Comparator<? super T> getComparator() {
    if (hasCharacteristics(SORTED)) return null;
    throw new IllegalStateException();
  }

  @Override
  public long estimateSize() {
    return est;
  }

  @Override
  public int characteristics() {
    return characteristics;
  }

  static final class HoldingConsumer<T> implements Consumer<T> {
    Object value;

    @Override
    public void accept(T value) {
      System.out.println("Accepting: " + value);
      this.value = value;
    }
  }
}