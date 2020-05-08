using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace LoadTestingFn
{
    public static class fnLoadTest
    {
        private static CosmosClient cosmosClient;

        [FunctionName("Function1")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            if(init++==0)
                generateData().Wait();
            
        }
        static int init = 0;
        private static async Task generateData()
        {
            //Reading configuration 
            string connectionString = Environment.GetEnvironmentVariable("connectionString");
            string dbName = Environment.GetEnvironmentVariable("database");
            string cnName = Environment.GetEnvironmentVariable("container");
            long.TryParse(Environment.GetEnvironmentVariable("numberOfDocumentsToGenerate"), out long noOfDocs);

            long.TryParse(Environment.GetEnvironmentVariable("perDocRUs"), out long perDocRUs);
            int.TryParse(Environment.GetEnvironmentVariable("maxRUs"), out int maxRUs);

            //long totalRURequired = Math.Abs(noOfDocs * perDocRUs);
            long numberOfDocsPerBatch = Math.Abs(Math.Abs(Convert.ToInt64(maxRUs * 0.90)) / perDocRUs);
            long batchSize = Math.Abs(noOfDocs / numberOfDocsPerBatch);

            CosmosClientBuilder cosmosClientBuilder =
             new CosmosClientBuilder(connectionString)
             .WithConnectionModeDirect()

             .WithBulkExecution(true);
            cosmosClient = cosmosClientBuilder.Build();

            Database db = await cosmosClient.CreateDatabaseIfNotExistsAsync(dbName);
            Container cn = await db.CreateContainerIfNotExistsAsync(cnName, "/id", maxRUs);

            List<List<Task>> tasks = new List<List<Task>>();
            for (int j = 0; j < batchSize; j++)
            {
                tasks.Add(CreateCustomer(numberOfDocsPerBatch, cn, j));
            }

            Stopwatch sw = new Stopwatch();
            for (int j = 0; j < tasks.Count; j++)
            {
                sw.Restart();
                await Task.WhenAll(tasks[j]);
                sw.Stop();
                Console.WriteLine("It took = " + sw.ElapsedMilliseconds / 1000 + "ms to insert " + tasks[j].Count +" records ");
                
                Thread.Sleep(sw.Elapsed);
                sw.Reset();
            }
        }

        private static List<Task> CreateCustomer(long numberOfDocsPerBatch, Container cn, int startIndex)
        {
            List<Task> tasks = new List<Task>();
            for (long i = Convert.ToInt64(startIndex) * numberOfDocsPerBatch; i < (numberOfDocsPerBatch * (startIndex + 1)); i++)
            {
                   tasks.Add(cn.CreateItemAsync(new CustomerModel()
                    {
                        Id = Guid.NewGuid().ToString(),
                        City = "city-" + i,
                        Name = "customer-" + i
                    }));

              
                Console.WriteLine(i.ToString());
            }
            return tasks;
        }
    }
}
