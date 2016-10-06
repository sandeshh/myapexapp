/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class LoggingRandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;
  private long windowId;

  public final transient DefaultOutputPort<Long> out = new DefaultOutputPort<Long>();

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    System.out.println(windowId);
    out.emit(windowId);
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
