using Avro;
using Avro.Generic;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kafka
{
    class Program
    {
        static void Main(string[] args)
        {
            string header = "kafka测试";

            Console.Title = header;
            Console.WriteLine(header);
            ConsoleColor color = Console.ForegroundColor;

            var pub = new ProduceHelper("Test", "192.168.0.230:9092,192.168.0.231:9092,192.168.0.232:9092,192.168.0.233:9092,192.168.0.234:9092", "192.168.0.230:8082");

            var sub = new ConsumerHelper("Test", "192.168.0.230:9092,192.168.0.231:9092,192.168.0.232:9092,192.168.0.233:9092,192.168.0.234:9092", "192.168.0.230:8082");
            //var s = (RecordSchema)Schema.Parse(
            //    @"{
            //        ""namespace"": ""kafka"",
            //        ""type"": ""record"",
            //        ""name"": ""User"",
            //        ""fields"": [
            //            {""name"": ""name"", ""type"": ""string""},
            //            {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
            //            {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
            //        ]
            //      }"
            //);
            //var record = new GenericRecord(s);
            //record.Add("name", "user");
            //record.Add("favorite_number", 5);
            //record.Add("favorite_color", "blue");
            User user = new User();
            user.name = "duzhengjie";
            user.favorite_number = 5;
            user.favorite_color = "blue";
            Task.Run(() =>
            {
                while (true)
                {
                    //string msg = string.Format("{0}这是一条测试消息", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    pub.Pub(user);
                }
            });

            Task.Run(() =>
            {
                sub.Sub();
            });

            Console.ReadLine();
        }
    }
}
