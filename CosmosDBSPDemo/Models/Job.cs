using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDBSPDemo.Models
{
    internal class Job : BaseEntity
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        public string JobDescription { get; set; }
        public string RetrievedBy { get; set; }
        public DateTime? RetrievedAt { get; set; }
    }

    internal class BaseEntity
    {
        public string _etag { get; set; }
    }
}
