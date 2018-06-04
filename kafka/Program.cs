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

            var pub = new ProduceHelper("Test", "");

            var sub = new ConsumerHelper("Test", "");

            Task.Run(() =>
            {
                while (true)
                {
                    string msg = string.Format("{0}这是一条测试消息", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    pub.Pub(new List<string> { msg });

                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("发送消息：" + msg);
                    //Console.ForegroundColor = color;
                    Thread.Sleep(2000);
                }
            });

            Task.Run(() => sub.Sub(msg =>
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("收到消息：{0}", msg);
                //Console.ForegroundColor = color;
            }));

            Console.ReadLine();
        }
    }
}
