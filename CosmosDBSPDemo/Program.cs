using CosmosDBSPDemo.Models;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDBSPDemo
{
    class Program
    {
        private const string CONFIG_FILENAME = "config.json";

        private static DocumentClient _documentClient;

        private static string _clientName = "Client1";
        private static string _documentDbEndpoint = "https://localhost:8081";
        private static string _documentDbKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private static string _databaseId = "DemoDB";
        private static string _collectionId = "DemoCollection";

        static void Main(string[] args)
        {
            Console.WriteLine("***************** COSMOS DEMO *****************");
            ReadConfigFile();

            _documentClient = new DocumentClient(new Uri(_documentDbEndpoint), _documentDbKey, new ConnectionPolicy
            {
                MaxConnectionLimit = 500
            });

            Console.WriteLine("Do you want to inject 1000 new records? (Y/N)");
            var toInject = "";
            do
            {
                Console.Write(">>> ");
                toInject = Console.ReadLine();
                if (toInject.ToUpper() == "Y")
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        Job job = new Job();
                        job.Id = Guid.NewGuid().ToString();
                        job.JobDescription = "Demo-" + job.Id;
                        job.RetrievedBy = string.Empty;
                        job.RetrievedAt = null;

                        AddAsync(job).GetAwaiter().GetResult();
                        Console.Write(".");
                    }
                }
            } while (toInject.ToUpper() != "Y" && toInject.ToUpper() != "N");
            Console.WriteLine("");



            Console.WriteLine("Do you want to set all records' RetrievedBy field to empty? (Y/N)");
            var toReset = "";
            do
            {
                Console.Write(">>> ");
                toReset = Console.ReadLine();
                if (toReset.ToUpper() == "Y")
                {
                    var recordList = GetAll().ToList();
                    foreach (var job in recordList)
                    {
                        job.RetrievedBy = string.Empty;
                        job.RetrievedAt = null;

                        UpdateAsync(job).GetAwaiter().GetResult();
                        Console.Write(".");
                    }
                }
            } while (toReset.ToUpper() != "Y" && toReset.ToUpper() != "N");
            Console.WriteLine("");

            Console.WriteLine(GetAll().Count() + " records found. ");
            Console.WriteLine("Start processing records in 5 secs. ");
            Thread.Sleep(5000);

            //List<Job> dataList = GetAll().ToList();

            //foreach(var data in dataList)
            //{
            //    Console.WriteLine("Job Id : " + data.JobId);
            //    Console.WriteLine("Job JobDescription : " + data.JobDescription);
            //    Console.WriteLine("Job RetrievedBy : " + data.RetrievedBy);
            //    Console.WriteLine("Job RetrievedAt : " + data.RetrievedAt);
            //}

            Thread thread = new Thread(() => ProcessJob(_clientName));
            thread.Start();

            Console.ReadLine();
        }

        private static bool ReadConfigFile()
        {
            try
            {
                var currentDirectory = Directory.GetCurrentDirectory() + "\\" + CONFIG_FILENAME;
                if (File.Exists(currentDirectory))
                {
                    var setting = JsonConvert.DeserializeObject<ConfigSetting>(File.ReadAllText(CONFIG_FILENAME));
                    if (setting != null)
                    {
                        _clientName = setting.ClientName;
                        string settingText = "--- SETTING --- "
                        + "\nClient Name\t: " + _clientName
                        + "\n\n";
                        Console.WriteLine(settingText);


                    }
                }
                return true;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        private static void ProcessJob(string clientName)
        {
            while (true)
            {
                var record = GetJobAsync("GetJob", clientName).GetAwaiter().GetResult();
                Job job = JsonConvert.DeserializeObject<Job>(record);
                if(job != null && job.Id != null)
                {
                    Console.WriteLine("****************************************************");
                    Console.WriteLine("Job Id : " + job.Id);
                    Console.WriteLine("Job Description : " + job.JobDescription);
                    Console.WriteLine("Job RetrievedBy : " + job.RetrievedBy);
                    Console.WriteLine("Job RetrievedAt : " + job.RetrievedAt);
                    Console.WriteLine("****************************************************\n");

                    job.JobDescription = job.JobDescription + "1";
                    UpdateAsync(job).GetAwaiter().GetResult();
                }
                
            }
        }

        private static async Task<string> GetJobAsync(string storedProcedure, string clientName)
        {
            var option = new FeedOptions { MaxItemCount = 100000, EnableCrossPartitionQuery = true };
            dynamic[] parameters = new dynamic[] { clientName };

            return await _documentClient.ExecuteStoredProcedureAsync<string>(UriFactory.CreateStoredProcedureUri(_databaseId, _collectionId, storedProcedure), parameters);
        }

        private static async Task AddAsync(Job job)
        {
            if (job == null)
            {
                throw new ArgumentNullException("job");
            }
            
            await CheckExistenceOfDatabaseAndCollectionAsync();

            await _documentClient.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), job);

        }

        private static IQueryable<Job> GetAll()
        {
            var option = new FeedOptions { MaxItemCount = 100000, EnableCrossPartitionQuery = true };

            return _documentClient.CreateDocumentQuery<Job>(UriFactory.CreateDocumentCollectionUri(_databaseId, _collectionId), option);
        }


        public static async Task UpdateAsync(Job job)
        {
            if (job == null)
            {
                throw new ArgumentNullException("job");
            }

            await CheckExistenceOfDatabaseAndCollectionAsync();

            var ac = GetAccessCondition(job);
            try
            {
                await _documentClient.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(_databaseId, _collectionId, job.Id), job,
                    new RequestOptions { AccessCondition = ac });
            }catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task CheckExistenceOfDatabaseAndCollectionAsync()
        {
            try
            {
                await _documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = _databaseId });
                await _documentClient.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(_databaseId), new DocumentCollection { Id = _collectionId });

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        private static AccessCondition GetAccessCondition(Job job)
        {
            return new AccessCondition { Condition = job._etag, Type = AccessConditionType.IfMatch };
        }
    }
}
