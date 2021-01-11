/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;

public class TimeUnits
{
  private static final Map<TimeUnitRange, Period> PERIOD_MAP = ImmutableMap.<TimeUnitRange, Period>builder()
      .put(TimeUnitRange.SECOND, Period.seconds(1))
      .put(TimeUnitRange.MINUTE, Period.minutes(1))
      .put(TimeUnitRange.HOUR, Period.hours(1))
      .put(TimeUnitRange.DAY, Period.days(1))
      .put(TimeUnitRange.WEEK, Period.weeks(1))
      .put(TimeUnitRange.MONTH, Period.months(1))
      .put(TimeUnitRange.QUARTER, Period.months(3))
      .put(TimeUnitRange.YEAR, Period.years(1))
      .build();

  /**
   * Returns the Druid QueryGranularity corresponding to a Calcite TimeUnitRange, or null if there is none.
   *
   * @param timeUnitRange time unit
   *
   * @return queryGranularity, or null
   */
  @Nullable
  public static Period toPeriod(final TimeUnitRange timeUnitRange)
  {
    return PERIOD_MAP.get(timeUnitRange);
  }
}
