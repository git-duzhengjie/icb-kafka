using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;

namespace kafka
{
    class ProduceHelper
    {
        private string _topicName;
        private string _brokerList;
        private string _schemaRegistryUrl;

        public ProduceHelper(string topicName, string brokerList, string schemaRegistryUrl)
        {
            _topicName = topicName;
            _brokerList = brokerList;
            _schemaRegistryUrl = schemaRegistryUrl;
        }


        public void Pub(User user)
        {

            var config = new Dictionary<string, object>
                {
                    { "bootstrap.servers", _brokerList },
                    { "schema.registry.url", _schemaRegistryUrl },
                };

            using (var producer = new Producer<Null, User>(config, null, new AvroSerializer<User>()))
            {
                var dr = producer.ProduceAsync(_topicName, null, user).Result;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Delivered '{dr.Value}' to: {dr.TopicPartitionOffset}");
            }
        }
    }
}
