using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace SendSampleData
{
    class Program
    {
        const string connectionString = "localhost:9092";
        private const string topic = "testing1";

        static void Main(string[] args)
        {
            EventHubIngestion();
        }

        static void EventHubIngestion()
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", connectionString }
            };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                int counter = 0;
                while (true)
                {
                    int recordsPerMessage = 3;
                    try
                    {
                        List<string> records = Enumerable
                            .Range(0, recordsPerMessage)
                            .Select(recordNumber =>
                                $"{{\"timeStamp\": \"{DateTime.UtcNow.AddSeconds(100 * counter)}\", \"name\": \"{$"name {counter}"}\", \"metric\": {counter + recordNumber}, \"source\": \"EventHubMessage\"}}")
                            .ToList();
                        string recordString = string.Join(Environment.NewLine, records);

                        Console.WriteLine($"sending message {counter}");
                        var dr = producer.ProduceAsync(topic, null, recordString).Result;
                        Console.WriteLine($"Delivered '{dr.Value}' to: {dr.TopicPartitionOffset}");
                    }
                    catch (Exception exception)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                        Console.ResetColor();
                    }

                    counter += recordsPerMessage;

                    Thread.Sleep(10000);
                }
            }
        }
    }
}