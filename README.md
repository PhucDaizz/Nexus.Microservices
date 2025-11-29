# SharedFramework 🏗️

SharedFramework được thiết kế tối ưu cho hệ thống Microservices. Thư viện cung cấp các thành phần cấu hình sẵn (pre-configured) giúp tích hợp RabbitMQ mạnh mẽ (tự động kết nối lại, chịu lỗi tốt) và chuẩn hóa API Response, giúp team tập trung vào logic nghiệp vụ thay vì tốn thời gian xử lý hạ tầng.

## 🚀 Tính năng nổi bật

### 🐰 Tích hợp RabbitMQ thông minh:

- **Lazy Initialization (Khởi tạo lười)**: Kết nối chỉ được mở khi thực sự cần thiết (gửi/nhận tin), giúp ứng dụng khởi động siêu nhanh, không bị treo.
- **Resilience (Khả năng phục hồi)**: Tích hợp sẵn Polly để tự động thử lại (Retry) khi mạng chập chờn hoặc RabbitMQ bị gián đoạn.
- **Thread-Safe**: An toàn tuyệt đối khi sử dụng với Singleton trong môi trường đa luồng.
- **Auto-Topology**: Tự động khởi tạo Exchange và Queue nếu chưa tồn tại.
- **Hỗ trợ đa Exchange Type**: Direct, Fanout, Topic, Headers

### 📦 Chuẩn hóa phản hồi API:

- `Result<T>` pattern thống nhất
- `ApiResponse<T>` với timestamp
- `PagedResult<T>` cho phân trang
- `BaseEntity` cho model cơ bản

### 🔧 Cài đặt siêu tốc:

Chỉ cần 1 dòng code cấu hình.

### 📦 Cài đặt

Cài đặt package thông qua NuGet Package Manager hoặc giao diện dòng lệnh (CLI):

```bash
dotnet add package SharedFrameworkFast
```

### Cấu hình

Thêm section RabbitMQ vào file `appsettings.json` của dự án:

```bash
{
  "RabbitMQ": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/"
  }
}
```

### 🛠️ Hướng dẫn sử dụng

Thêm section RabbitMQ vào file `appsettings.json` của dự án:
1. Đăng ký Service

Trong file `Program.cs`, gọi hàm extension để đăng ký cả Publisher và Consumer:
```bash
using SharedLibrary.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Đăng ký RabbitMQ (Consumer & Publisher) 
builder.Services.AddSharedRabbitMQ(builder.Configuration);

var app = builder.Build();
```

2. Gửi tin nhắn (Publishing) 📤
   
Inject interface `IMessagePublisher` vào Controller hoặc Service của bạn. Bạn không cần lo lắng về việc mở kết nối, thư viện sẽ tự xử lý.

```bash
public class OrdersController : ControllerBase
{
    private readonly IMessagePublisher _publisher;

    public OrdersController(IMessagePublisher publisher)
    {
        _publisher = publisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(OrderDto order)
    {
        // Cách 1: Gửi thẳng vào Queue (Tự động tạo Queue nếu chưa có)
        await _publisher.PublishAsync("orders-queue", order);

        // Cách 2: Gửi vào Exchange kèm Routing Key (Cho Pub/Sub pattern)
        await _publisher.PublishAsync("orders-exchange", ExchangeType.Topic, "order.created", order);

        return Ok();
    }
}
```

3. Nhận tin nhắn (Consuming) 📥

Để nhận tin nhắn, hãy tạo một BackgroundService.

```⚠️ QUAN TRỌNG: Luôn đặt logic đăng ký (Subscribe) bên trong hàm ExecuteAsync và sử dụng Task.Yield() hoặc để nó chạy ngầm. TUYỆT ĐỐI KHÔNG đặt trong StartAsync vì sẽ làm treo ứng dụng (block Swagger).```
```bash
using SharedLibrary.Interfaces;

public class OrderProcessingService : BackgroundService
{
    private readonly IMessageConsumer _consumer;
    private readonly ILogger<OrderProcessingService> _logger;

    public OrderProcessingService(IMessageConsumer consumer, ILogger<OrderProcessingService> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); 

        // 1. Đăng ký lắng nghe Queue đơn giản
        await _consumer.Subscribe<OrderDto>("orders-queue", HandleOrderCreated);

        // 2. Đăng ký lắng nghe Exchange phức tạp
        await _consumer.Subscribe<OrderDto>("orders-exchange", ExchangeType.Topic, "order.created", "orders-queue", HandleOrderCreated);
        
        // Giữ service chạy ngầm mãi mãi
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private Task HandleOrderCreated(OrderDto order)
    {
        _logger.LogInformation($"Đang xử lý đơn hàng: {order.OrderId}");
        return Task.CompletedTask;
    }
}
```

### Các loại Exchange hỗ trợ
```bash
// Direct Exchange - routing chính xác
ExchangeType.Direct

// Fanout Exchange - broadcast tất cả queue
ExchangeType.Fanout

// Topic Exchange - routing pattern matching
ExchangeType.Topic

// Headers Exchange - routing dựa trên headers
ExchangeType.Headers
```

### Đừng quên đăng ký Worker trong Program.cs:

```bash
builder.Services.AddHostedService<OrderProcessingService>();
```

## 📝 Mẫu phản hồi API Response:

```bash
// Success
return Result<string>.Success("Thành công");

// Error  
return Result<string>.Failure(new Error("VALIDATION", "Lỗi validation"));

// ApiResponse
return ApiResponse<User>.SuccessResponse(user, "Tạo thành công");

// PagedResult
return PagedResult<User>.Create(users, totalCount, pageNumber, pageSize);
```

## 📄 Models

BaseEntity
```bash
public abstract class BaseEntity
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }
    public bool IsDeleted { get; set; } = false;
}
```
## 🤝 Đóng góp
Mọi đóng góp đều được hoan nghênh để làm thư viện tốt hơn cho team!

- Fork repository này.

- Tạo branch tính năng mới (git checkout -b feature/tinh-nang-moi).

- Commit code của bạn.

- Push lên branch.

- Tạo Pull Request.
