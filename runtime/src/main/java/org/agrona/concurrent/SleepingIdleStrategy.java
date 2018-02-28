/*
 *  Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.agrona.concurrent;

import org.agrona.hints.ThreadHints;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * When idle this strategy is to sleep for a specified period in nanoseconds.
 *
 * This class uses {@link LockSupport#parkNanos(long)} to idle.
 */
abstract class BackoffIdleStrategyPrePad
{
    long pad01, pad02, pad03, pad04, pad05, pad06, pad07;
}

abstract class BackoffIdleStrategyData extends BackoffIdleStrategyPrePad
{
    enum State
    {
        NOT_IDLE, SPINNING, YIELDING, PARKING
    }

    protected final long maxSpins;
    protected final long maxYields;
    protected final long minParkPeriodNs;
    protected final long maxParkPeriodNs;

    protected State state;

    protected long spins;
    protected long yields;
    protected long parkPeriodNs;

    BackoffIdleStrategyData(
            final long maxSpins, final long maxYields, final long minParkPeriodNs, final long maxParkPeriodNs)
    {
        this.maxSpins = maxSpins;
        this.maxYields = maxYields;
        this.minParkPeriodNs = minParkPeriodNs;
        this.maxParkPeriodNs = maxParkPeriodNs;
    }
}

public final class SleepingIdleStrategy extends BackoffIdleStrategyData implements IdleStrategy
{

    long pad01, pad02, pad03, pad04, pad05, pad06, pad07;

    /**
     * Constructed a new strategy that will sleep for a given period when idle.
     *
     * @param sleepPeriodNs period in nanosecond for which the strategy will sleep when work count is 0.
     */
    public SleepingIdleStrategy(final long sleepPeriodNs)
    {
        super(5, 5, 1, TimeUnit.MILLISECONDS.toNanos(1));
        System.out.println("Ama winner");
        this.state = State.NOT_IDLE;
    }

    public void idle(final int workCount)
    {
        if (workCount > 0)
        {
            reset();
        }
        else
        {
            idle();
        }
    }

    public void idle()
    {
        switch (state)
        {
            case NOT_IDLE:
                state = State.SPINNING;
                spins++;
                break;

            case SPINNING:
                ThreadHints.onSpinWait();
                if (++spins > maxSpins)
                {
                    state = State.YIELDING;
                    yields = 0;
                }
                break;

            case YIELDING:
                if (++yields > maxYields)
                {
                    state = State.PARKING;
                    parkPeriodNs = minParkPeriodNs;
                }
                else
                {
                    Thread.yield();
                }
                break;

            case PARKING:
                LockSupport.parkNanos(parkPeriodNs);
                parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs);
                break;
        }
    }

    public void reset()
    {
        spins = 0;
        yields = 0;
        state = State.NOT_IDLE;
    }
}
