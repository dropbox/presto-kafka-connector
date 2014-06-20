/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dropbox.presto.kafka;

public class KafkaTableProperties
{

    public static String kafkaTableMarker = "kafka_table";

    public static String kafkaTopicName = "kafka_topic";

    public static String kafkaPartTsColumn = "kafka_ts_part_col";

    public static String kafkaSplitRange = "kafka_split_range";

    public static String kafkaJobRange = "kafka_job_range";

    public static String kafkaTableSampleRate = "kafka_sample_rate";

}
