using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SharedLibrary.Configuration;
using SharedLibrary.Interfaces;
using SharedLibrary.Services;

namespace SharedLibrary.Extensions
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
