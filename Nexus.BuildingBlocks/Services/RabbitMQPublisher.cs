using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nexus.BuildingBlocks.Configuration;
using Nexus.BuildingBlocks.Interfaces;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;

namespace Nexus.BuildingBlocks.Services
{
    public class RabbitMQPublisher : IMessagePublisher, IAsyncDisposable
    {
        private readonly RabbitMQSettings _settings;
        private readonly ILogger<RabbitMQPublisher> _logger;
        private IConnection? _connection;
        private IChannel? _channel;

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<string, bool> _declaredExchanges = new();
        private readonly ConcurrentDictionary<string, bool> _declaredQueues = new();

        private readonly JsonSerializerOptions? _jsonOptions;
        private readonly AsyncRetryPolicy _retryPolicy;

        public RabbitMQPublisher(IOptions<RabbitMQSettings> settings,
            ILogger<RabbitMQPublisher> logger,
            JsonSerializerOptions? jsonOptions = null)
        {
            _settings = settings.Value;
            _logger = logger;
            _jsonOptions = jsonOptions;

            if (_jsonOptions == null)
            {
                _jsonOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All), 
                    WriteIndented = false,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                    Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
                };
            }

            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogWarning($"Publisher cannot connect (Attempt {retryCount}). Retrying in {timeSpan.TotalSeconds}s...");
                    });
        }

        private async Task EnsureConnectionAsync()
        {
            if (_connection != null && _connection.IsOpen && _channel != null && _channel.IsOpen) return;

            await _connectionLock.WaitAsync();

            try
            {
                if (_connection != null && _connection.IsOpen && _channel != null && _channel.IsOpen) return;

                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation("Connecting RabbitMQ Publisher...");
                    var factory = new ConnectionFactory
                    {
                        HostName = _settings.HostName,
                        Port = _settings.Port,
                        UserName = _settings.UserName,
                        Password = _settings.Password,
                        VirtualHost = _settings.VirtualHost,
                        AutomaticRecoveryEnabled = true
                    };

                    _connection = await factory.CreateConnectionAsync();
                    _channel = await _connection.CreateChannelAsync();
                    _declaredExchanges.Clear();
                    _declaredQueues.Clear();

                    _logger.LogInformation("RabbitMQ Publisher connected.");
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to connect RabbitMQ Publisher.");
                throw; 
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        public async Task PublishAsync<T>(string exchange, string exchangeType, string routingKey, T message)
        {
            await EnsureConnectionAsync();

            try
            {
                if (!_declaredExchanges.ContainsKey(exchange))
                {
                    await _channel!.ExchangeDeclareAsync(exchange, exchangeType, durable: true);
                    _declaredExchanges.TryAdd(exchange, true);
                }

                var json = JsonSerializer.Serialize(message, _jsonOptions);
                var body = Encoding.UTF8.GetBytes(json);

                var properties = new BasicProperties
                {
                    Persistent = true,
                    ContentType = "application/json; charset=utf-8",
                    MessageId = Guid.NewGuid().ToString(),
                    Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                    Headers = new Dictionary<string, object>
                    {
                        { "Message-Type", typeof(T).Name },
                        { "Created-At", DateTime.UtcNow.ToString("o") }
                    }
                };

                await _channel!.BasicPublishAsync(
                    exchange: exchange,
                    routingKey: routingKey,
                    mandatory: false,
                    basicProperties: properties,
                    body: body
                );

                _logger.LogDebug("Published message to Exchange: {Exchange}, RoutingKey: {RoutingKey}",
                    exchange, routingKey);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error publishing message to Exchange: {Exchange}", exchange);
                throw;
            }
        }

        public async Task PublishAsync<T>(string queueName, T message)
        {
            await EnsureConnectionAsync();

            try
            {
                if (!_declaredQueues.ContainsKey(queueName))
                {
                    await _channel!.QueueDeclareAsync(
                        queue: queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    _declaredQueues.TryAdd(queueName, true);
                }

                var json = JsonSerializer.Serialize(message, _jsonOptions);
                var body = Encoding.UTF8.GetBytes(json);

                var properties = new BasicProperties
                {
                    Persistent = true,
                    ContentType = "application/json; charset=utf-8",
                    MessageId = Guid.NewGuid().ToString(),
                    Headers = new Dictionary<string, object>
                    {
                        { "Message-Type", typeof(T).Name },
                        { "Created-At", DateTime.UtcNow.ToString("o") }
                    }
                };

                await _channel!.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: queueName,
                    mandatory: false,
                    basicProperties: properties,
                    body: body
                );

                _logger.LogDebug("Published message to Queue: {Queue}", queueName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error publishing message to Queue: {Queue}", queueName);
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_channel != null) await _channel.CloseAsync();
            if (_connection != null) await _connection.CloseAsync();
            _connectionLock.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}