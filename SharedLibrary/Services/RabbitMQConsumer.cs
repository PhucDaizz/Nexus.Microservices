using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedLibrary.Configuration;
using SharedLibrary.Interfaces;
using System.Text;
using System.Text.Json;

namespace SharedLibrary.Services
{
    public class RabbitMQConsumer : IMessageConsumer, IAsyncDisposable
    {
        private readonly RabbitMQSettings _settings;
        private readonly ILogger<RabbitMQConsumer> _logger;
        private IConnection? _connection;
        private IChannel? _channel;

        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
        private bool _isInitialized = false;

        private readonly AsyncRetryPolicy _retryPolicy;

        public RabbitMQConsumer(IOptions<RabbitMQSettings> settings, ILogger<RabbitMQConsumer> logger)
        {
            _settings = settings.Value;
            _logger = logger;

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
                try
                {
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    var message = JsonSerializer.Deserialize<T>(json);

                    if (message != null)
                    {
                        await handler(message);
                        await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                    }
                    else
                    {
                        await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message");
                    await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            await _channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);
            _logger.LogInformation($"Listening on Queue: {queueName}");
        }

        public async ValueTask DisposeAsync()
        {
            if (_channel != null) await _channel.CloseAsync();
            if (_connection != null) await _connection.CloseAsync();
            _connectionLock.Dispose();
        }
    }
}