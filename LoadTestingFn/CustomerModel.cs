using Newtonsoft.Json;

namespace LoadTestingFn
{
        class CustomerModel
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("city")]
            public string City { get; set; }
        }
}