presto-kafka-connector
======================

This is presto connector for kafka. Kakfa is a real time data logging system. And presto is a very low latency query engine. With the presto kafka connector, it allows you to query kafka data with presto. So basically it enables very low latency complex quering of real time logging.

To use the connector, it requires a small change to kafka. You can checkout the kafka diff here: https://www.dropbox.com/s/degcv61d9002wvg/kafaka_presto_connector.diff

or you can download the kafka jar that we compiled with the diff here: https://www.dropbox.com/s/zg5fgycmvz9w3l5/kafka_2.10-0.8.1.jar

