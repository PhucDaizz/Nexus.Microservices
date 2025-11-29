using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Nexus.BuildingBlocks.Configuration;
using Nexus.BuildingBlocks.Interfaces;
using Nexus.BuildingBlocks.Services;

namespace Nexus.BuildingBlocks.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddSharedRabbitMQ(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            services.Configure<RabbitMQSettings>(configuration.GetSection(RabbitMQSettings.SectionName));

            services.AddSingleton<RabbitMQConsumer>();
            services.AddSingleton<IMessageConsumer>(sp => sp.GetRequiredService<RabbitMQConsumer>());

            services.AddSingleton<RabbitMQPublisher>();
            services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<RabbitMQPublisher>());

            return services;
        }
    }
}
