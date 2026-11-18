/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "DateUtils.hh"

#include <cstdio>
#include <stdexcept>

namespace orc {
  namespace {
    constexpr int64_t UNIX_EPOCH_JDN = 2440588;
    constexpr int32_t SWITCHOVER_DAYS = -141427;  // 1582-10-15
    constexpr int64_t MILLIS_PER_DAY = 24LL * 60 * 60 * 1000;

    struct CivilDate {
      int32_t year;
      int32_t month;
      int32_t day;
    };

    int64_t gregorianJdn(int32_t year, int32_t month, int32_t day) {
      const int32_t a = (14 - month) / 12;
      const int64_t y = static_cast<int64_t>(year) + 4800 - a;
      const int32_t m = month + 12 * a - 3;
      return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    }

    int64_t julianJdn(int32_t year, int32_t month, int32_t day) {
      const int32_t a = (14 - month) / 12;
      const int64_t y = static_cast<int64_t>(year) + 4800 - a;
      const int32_t m = month + 12 * a - 3;
      return day + (153 * m + 2) / 5 + 365 * y + y / 4 - 32083;
    }

    CivilDate gregorianFromJdn(int64_t jdn) {
      const int64_t a = jdn + 32044;
      const int64_t b = (4 * a + 3) / 146097;
      const int64_t c = a - (146097 * b) / 4;
      const int64_t d = (4 * c + 3) / 1461;
      const int64_t e = c - (1461 * d) / 4;
      const int64_t m = (5 * e + 2) / 153;
      return {static_cast<int32_t>(100 * b + d - 4800 + m / 10),
              static_cast<int32_t>(m + 3 - 12 * (m / 10)),
              static_cast<int32_t>(e - (153 * m + 2) / 5 + 1)};
    }

    CivilDate julianFromJdn(int64_t jdn) {
      const int64_t c = jdn + 32082;
      const int64_t d = (4 * c + 3) / 1461;
      const int64_t e = c - (1461 * d) / 4;
      const int64_t m = (5 * e + 2) / 153;
      return {static_cast<int32_t>(d - 4800 + m / 10),
              static_cast<int32_t>(m + 3 - 12 * (m / 10)),
              static_cast<int32_t>(e - (153 * m + 2) / 5 + 1)};
    }

    bool isOnOrAfterGregorianCutover(const CivilDate& date) {
      if (date.year != 1582) return date.year > 1582;
      if (date.month != 10) return date.month > 10;
      return date.day >= 15;
    }

    CivilDate parseDate(const std::string& date) {
      CivilDate result {0, 0, 0};
      if (std::sscanf(date.c_str(), "%d-%d-%d", &result.year, &result.month, &result.day) != 3) {
        throw std::invalid_argument("Invalid date: " + date);
      }
      return result;
    }

    int64_t floorDivide(int64_t value, int64_t divisor) {
      int64_t quotient = value / divisor;
      const int64_t remainder = value % divisor;
      if (remainder != 0 && ((remainder < 0) != (divisor < 0))) {
        --quotient;
      }
      return quotient;
    }
  }  // namespace

  int32_t parseHybridDate(const std::string& date) {
    const CivilDate civilDate = parseDate(date);
    const int64_t jdn = isOnOrAfterGregorianCutover(civilDate)
                                ? gregorianJdn(civilDate.year, civilDate.month, civilDate.day)
                                : julianJdn(civilDate.year, civilDate.month, civilDate.day);
    return static_cast<int32_t>(jdn - UNIX_EPOCH_JDN);
  }

  int32_t parseProlepticDate(const std::string& date) {
    const CivilDate civilDate = parseDate(date);
    return static_cast<int32_t>(
        gregorianJdn(civilDate.year, civilDate.month, civilDate.day) - UNIX_EPOCH_JDN);
  }

  int32_t convertDateToProleptic(int32_t hybrid) {
    if (hybrid >= SWITCHOVER_DAYS) {
      return hybrid;
    }
    const CivilDate hybridDate = julianFromJdn(static_cast<int64_t>(hybrid) + UNIX_EPOCH_JDN);
    return static_cast<int32_t>(
        gregorianJdn(hybridDate.year, hybridDate.month, hybridDate.day) - UNIX_EPOCH_JDN);
  }

  int32_t convertDateToHybrid(int32_t proleptic) {
    if (proleptic >= SWITCHOVER_DAYS) {
      return proleptic;
    }
    const CivilDate prolepticDate =
        gregorianFromJdn(static_cast<int64_t>(proleptic) + UNIX_EPOCH_JDN);
    return static_cast<int32_t>(
        julianJdn(prolepticDate.year, prolepticDate.month, prolepticDate.day) - UNIX_EPOCH_JDN);
  }

  int32_t convertDate(int32_t original, bool fromProleptic, bool toProleptic) {
    if (fromProleptic == toProleptic) {
      return original;
    }
    return toProleptic ? convertDateToProleptic(original) : convertDateToHybrid(original);
  }

  int64_t convertTimeToProleptic(int64_t hybridMillis) {
    const int64_t hybridDay = floorDivide(hybridMillis, MILLIS_PER_DAY);
    const int64_t millisOfDay = hybridMillis - hybridDay * MILLIS_PER_DAY;
    return static_cast<int64_t>(convertDateToProleptic(static_cast<int32_t>(hybridDay))) *
                   MILLIS_PER_DAY +
           millisOfDay;
  }

  int64_t convertTimeToHybrid(int64_t prolepticMillis) {
    const int64_t prolepticDay = floorDivide(prolepticMillis, MILLIS_PER_DAY);
    const int64_t millisOfDay = prolepticMillis - prolepticDay * MILLIS_PER_DAY;
    return static_cast<int64_t>(convertDateToHybrid(static_cast<int32_t>(prolepticDay))) *
                   MILLIS_PER_DAY +
           millisOfDay;
  }

  int64_t convertTime(int64_t originalMillis, bool fromProleptic, bool toProleptic) {
    if (fromProleptic == toProleptic) {
      return originalMillis;
    }
    return toProleptic ? convertTimeToProleptic(originalMillis)
                       : convertTimeToHybrid(originalMillis);
  }

}  // namespace orc
