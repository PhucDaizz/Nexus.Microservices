using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nexus.BuildingBlocks.Interfaces
{
    public interface IMessagePublisher
    {
        /// <summary>
        /// Phương thức bất đồng bộ để xuất bản một tin nhắn đến một exchange và routing key cụ thể trong hệ thống RabbitMQ.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exchange"></param>
        /// <param name="exchangeType"></param>
        /// <param name="routingKey"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        Task PublishAsync<T>(string exchange, string exchangeType, string routingKey, T message);

        /// <summary>
        /// Phương thức bất đồng bộ để xuất bản một tin nhắn đến một hàng đợi cụ thể trong hệ thống RabbitMQ.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queueName"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        Task PublishAsync<T>(string queueName, T message);
    }
}
