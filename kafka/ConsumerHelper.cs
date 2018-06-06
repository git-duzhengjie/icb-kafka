using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace kafka
{
    class ConsumerHelper
    {
        private string _topics;
        private string _brokerList;
        private string _schemaRegistryUrl;

        public ConsumerHelper(string topics, string brokerList, string schemaRegistryUrl)
        {
            _topics = topics ;
            _brokerList = brokerList;
            _schemaRegistryUrl = schemaRegistryUrl;
        }

        public void Sub()
        {
            var conf = new Dictionary<string, object>
                {
                  { "group.id", "test-consumer-group" },
                  { "bootstrap.servers", _brokerList },
                  { "auto.commit.interval.ms", 5000 },
                  { "auto.offset.reset", "earliest" },
                  { "schema.registry.url", _schemaRegistryUrl }
                };

            using (var consumer = new Consumer<Null, User>(conf, null, new AvroDeserializer<User>()))
            {
                
                consumer.OnMessage += (_, e)
                  => Console.WriteLine($"Key: {e.Key}\nValue: {e.Value}");

                consumer.OnError += (_, error)
                  => Console.WriteLine($"Error: {error}");

                consumer.OnConsumeError += (_, msg)
                  => Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.Subscribe(_topics);

                while (true)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}
