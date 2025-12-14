using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nexus.BuildingBlocks.Configuration;
using Nexus.BuildingBlocks.Interfaces;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;

namespace Nexus.BuildingBlocks.Services
{
    public class RabbitMQConsumer : IMessageConsumer, IAsyncDisposable
    {
        private readonly RabbitMQSettings _settings;
        private readonly ILogger<RabbitMQConsumer> _logger;
        private IConnection? _connection;
        private IChannel? _channel;

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
        private bool _isInitialized = false;

        private readonly JsonSerializerOptions? _jsonOptions;
        private readonly AsyncRetryPolicy _retryPolicy;

        public RabbitMQConsumer(
            IOptions<RabbitMQSettings> settings,
            ILogger<RabbitMQConsumer> logger,
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
                    retryCount: 5,
                    sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogWarning($"RabbitMQ not ready (Attempt {retryCount}). Retrying in {timeSpan.TotalSeconds}s... Error: {exception.Message}");
                    });
        }

        private async Task EnsureConnectionAsync()
        {
            if (_isInitialized && _connection != null && _connection.IsOpen) return;

            await _connectionLock.WaitAsync();

            try
            {
                if (_isInitialized && _connection != null && _connection.IsOpen) return;

                await _retryPolicy.ExecuteAsync(async () =>
                {
                    _logger.LogInformation("Connecting to RabbitMQ...");
                    var factory = new ConnectionFactory
                    {
                        HostName = _settings.HostName,
                        Port = _settings.Port,
                        UserName = _settings.UserName,
                        Password = _settings.Password,
                        VirtualHost = _settings.VirtualHost,
                        AutomaticRecoveryEnabled = true, 
                        NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                    };

                    _connection = await factory.CreateConnectionAsync();
                    _channel = await _connection.CreateChannelAsync();
                    await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

                    _isInitialized = true;
                    _logger.LogInformation("RabbitMQ Consumer connected successfully.");
                });
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "FATAL: Could not connect to RabbitMQ after multiple attempts.");
                throw; 
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        public async Task Subscribe<T>(string queueName, Func<T, Task> handler)
        {
            await EnsureConnectionAsync();

            await _channel!.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
            await StartConsumerAsync(queueName, handler);
        }

        public async Task Subscribe<T>(string exchange, string exchangeType, string routingKey, string queueName, Func<T, Task> handler)
        {
            await EnsureConnectionAsync();

            await _channel!.ExchangeDeclareAsync(exchange: exchange, type: exchangeType, durable: true, autoDelete: false);
            await _channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
            await _channel.QueueBindAsync(queue: queueName, exchange: exchange, routingKey: routingKey);

            await StartConsumerAsync(queueName, handler, exchange, routingKey);
        }

        private async Task StartConsumerAsync<T>(string queueName, Func<T, Task> handler, string? exchange = null, string? routingKey = null)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel!);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                string? messageJson = null;
                try
                {
                    var body = ea.Body.ToArray();
                    messageJson = Encoding.UTF8.GetString(body);

                    _logger.LogDebug("Received message from {Queue}. DeliveryTag: {DeliveryTag}, MessageId: {MessageId}",
                        queueName,
                        ea.DeliveryTag,
                        ea.BasicProperties.MessageId);

                    var message = JsonSerializer.Deserialize<T>(messageJson, _jsonOptions);

                    if (message == null)
                    {
                        _logger.LogError("Failed to deserialize message from queue {Queue}. JSON: {Json}",
                            queueName, messageJson);

                        await _channel.BasicNackAsync(
                            ea.DeliveryTag,
                            multiple: false,
                            requeue: false); 
                        return;
                    }

                    await handler(message);
                    await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);


                    _logger.LogDebug("Successfully processed message from {Queue}. DeliveryTag: {DeliveryTag}",
                        queueName, ea.DeliveryTag);
                }
                catch (JsonException jsonEx)
                {
                    _logger.LogError(jsonEx,
                        "JSON deserialization error for message in queue {Queue}. JSON: {Json}",
                        queueName, messageJson);

                    await _channel.BasicNackAsync(
                        ea.DeliveryTag,
                        multiple: false,
                        requeue: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Error processing message in queue {Queue}. DeliveryTag: {DeliveryTag}",
                        queueName, ea.DeliveryTag);

                    await _channel.BasicNackAsync(
                        ea.DeliveryTag,
                        multiple: false,
                        requeue: true); 
                }
            };

            await _channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: false,
                consumer: consumer);
            _logger.LogInformation($"Listening on Queue: {queueName}");
        }

        public async ValueTask DisposeAsync()
        {
            if (_channel != null)
            {
                await _channel.CloseAsync();
                await _channel.DisposeAsync();
            }

            if (_connection != null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
            }

            _connectionLock.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}