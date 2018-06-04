using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka
{
    class ProduceHelper 
    {
        private string _topicName;
        private string _brokerList;

        public ProduceHelper(string topicName, string brokerList)
        {
            _topicName = topicName;
            _brokerList = brokerList;
        }

        /// <summary>
        ///  发送消息到队列
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="datas"></param>
        /// <param name="acks"></param>
        /// <param name="timeout"></param>
        /// <param name="codec"></param>
        public void Pub(List<string> datas)
        {
           
            var config = new Dictionary<string, object> { { "bootstrap.servers", _brokerList } };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))

                foreach (string ms in datas)
                {
                    var deliveryReport = producer.ProduceAsync(_topicName, null, ms);
                    deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });
                }
            }
        }
}
