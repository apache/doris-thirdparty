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

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestDateUtils, convertHybridDateToProlepticMatchesJavaDateUtils) {
    EXPECT_EQ(16768, convertDate(16768, false, true));
    EXPECT_EQ(-141427, convertDate(-141427, false, true));
    EXPECT_EQ(-141438, convertDate(-141428, false, true));
    EXPECT_EQ(-499955, convertDate(-499952, false, true));
  }

  TEST(TestDateUtils, convertProlepticDateToHybridIsInverse) {
    EXPECT_EQ(16768, convertDate(16768, true, false));
    EXPECT_EQ(-141427, convertDate(-141427, true, false));
    EXPECT_EQ(-141428, convertDate(-141438, true, false));
    EXPECT_EQ(-499952, convertDate(-499955, true, false));
  }

  TEST(TestDateUtils, convertHiveLegacyReproDatesToProleptic) {
    EXPECT_EQ(parseProlepticDate("0002-01-01"), convertDate(parseHybridDate("0002-01-01"),
                                                            false, true));
    EXPECT_EQ(parseProlepticDate("1500-01-01"), convertDate(parseHybridDate("1500-01-01"),
                                                            false, true));
    EXPECT_EQ(parseProlepticDate("1582-10-04"), convertDate(parseHybridDate("1582-10-04"),
                                                            false, true));
    EXPECT_EQ(parseProlepticDate("1582-11-04"), convertDate(parseHybridDate("1582-11-04"),
                                                            false, true));
    EXPECT_EQ(parseProlepticDate("2000-02-29"), convertDate(parseHybridDate("2000-02-29"),
                                                            false, true));
  }

  TEST(TestDateUtils, convertHiveLegacyReproTimestampsToProleptic) {
    const int64_t millisInDay = 24 * 60 * 60 * 1000;
    const int64_t millis = 123;

    EXPECT_EQ(parseProlepticDate("0002-01-01") * millisInDay + millis,
              convertTime(parseHybridDate("0002-01-01") * millisInDay + millis, false, true));
    EXPECT_EQ(parseProlepticDate("1500-01-01") * millisInDay + millis,
              convertTime(parseHybridDate("1500-01-01") * millisInDay + millis, false, true));
    EXPECT_EQ(parseProlepticDate("1582-10-04") * millisInDay + millis,
              convertTime(parseHybridDate("1582-10-04") * millisInDay + millis, false, true));
    EXPECT_EQ(parseProlepticDate("1582-11-04") * millisInDay + millis,
              convertTime(parseHybridDate("1582-11-04") * millisInDay + millis, false, true));
    EXPECT_EQ(parseProlepticDate("2000-02-29") * millisInDay + millis,
              convertTime(parseHybridDate("2000-02-29") * millisInDay + millis, false, true));
  }

}  // namespace orc
