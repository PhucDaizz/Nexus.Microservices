using System;
namespace Nexus.BuildingBlocks.Interfaces
{
    public interface IMessageConsumer
    {
        /// <summary>
        /// Phương thức bất đồng bộ để đăng ký một trình xử lý tin nhắn cho một hàng đợi cụ thể trong hệ thống RabbitMQ.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queueName"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        Task Subscribe<T>(string queueName, Func<T, Task> handler);

        /// <summary>
        /// Phương thức bất đồng bộ để đăng ký một trình xử lý tin nhắn cho một exchange và routing key cụ thể trong hệ thống RabbitMQ.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exchange"></param>
        /// <param name="exchangeType"></param>
        /// <param name="routingKey"></param>
        /// <param name="queueName"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        Task Subscribe<T>(string exchange, string exchangeType, string routingKey, string queueName, Func<T, Task> handler);
    }
}
