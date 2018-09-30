/*
 *    Copyright 2018 Timothy J Veil
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package veil.hdp.hive.jdbc;

import com.google.common.base.Joiner;
import org.apache.commons.text.RandomStringGenerator;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TestDataGenerator {
    public static void main(String[] args) {

        RandomStringGenerator stringGenerator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

        for (int i = 0; i < 1000; i++) {
            List<String> row = new ArrayList<>(13);

            String randomByte = Integer.toString(ThreadLocalRandom.current().nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE));
            String randomShort = Integer.toString(ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE));
            int randomInt = ThreadLocalRandom.current().nextInt();
            long randomLong = ThreadLocalRandom.current().nextLong();
            boolean randomBoolean = ThreadLocalRandom.current().nextBoolean();
            float randomFloat = ThreadLocalRandom.current().nextFloat();
            double randomDouble = ThreadLocalRandom.current().nextDouble();

            String randomString = stringGenerator.generate(25);
            String randomVarchar = stringGenerator.generate(10);
            Timestamp randomTimestamp = Timestamp.valueOf(randomTimestamp());
            Date randomDate = Date.valueOf(randomDate());
            char randomChar = stringGenerator.generate(1).charAt(0);

            BigDecimal randomBig = new BigDecimal(ThreadLocalRandom.current().nextDouble());
            randomBig = randomBig.setScale(10, BigDecimal.ROUND_DOWN);

            row.add(Byte.toString(Byte.parseByte(randomByte)));
            row.add(Short.toString(Short.parseShort(randomShort)));
            row.add(Integer.toString(randomInt));
            row.add(Long.toString(randomLong));
            row.add(Boolean.toString(randomBoolean));
            row.add(Float.toString(randomFloat));
            row.add(Double.toString(randomDouble));
            row.add(randomString);
            row.add(randomTimestamp.toString());
            row.add(randomBig.toString());
            row.add(randomVarchar);
            row.add(randomDate.toString());
            row.add(Character.toString(randomChar));

            String rowString = Joiner.on(',').join(row);

            System.out.println(rowString);

        }

    }

    private static LocalDate randomDate() {
        long minDay = LocalDate.of(1970, 1, 1).toEpochDay();
        long maxDay = LocalDate.of(2017, 12, 31).toEpochDay();
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return LocalDate.ofEpochDay(randomDay);
    }

    private static LocalDateTime randomTimestamp() {
        long minDay = LocalDateTime.of(1970, 1, 1, 1, 1).toEpochSecond(ZoneOffset.UTC);
        long maxDay = LocalDateTime.of(2017, 12, 31, 1, 1).toEpochSecond(ZoneOffset.UTC);
        long randomDay = ThreadLocalRandom.current().nextLong(minDay, maxDay);
        return LocalDateTime.ofEpochSecond(randomDay, 0, ZoneOffset.UTC);
    }
}
