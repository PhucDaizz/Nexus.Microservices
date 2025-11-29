namespace Nexus.BuildingBlocks.Configuration
{
    public class RabbitMQSettings
    {
        public const string SectionName = "RabbitMQ";
        public string? HostName { get; set; } = "localhost";
        public int Port { get; set; } = 5672;
        public string? UserName { get; set; } = "guest";
        public string? Password { get; set; } = "guest";
        public string? VirtualHost { get; set; } = "/";
        public int? RetryCount { get; set; } = 3;
        public int? RetryDelaySeconds { get; set; } = 5;
    }
}
