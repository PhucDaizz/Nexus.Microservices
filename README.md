# Nexus.Microservices 🏗️

Nexus.Microservices được thiết kế tối ưu cho hệ thống Microservices. Thư viện cung cấp các thành phần cấu hình sẵn (pre-configured) giúp tích hợp RabbitMQ mạnh mẽ (tự động kết nối lại, chịu lỗi tốt) và chuẩn hóa API Response, giúp team tập trung vào logic nghiệp vụ thay vì tốn thời gian xử lý hạ tầng.

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
dotnet add package Nexus.BuildingBlocks
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
    "VirtualHost": "/",
    "RetryCount": 3,
    "RetryDelaySeconds": 5
  }
}
```

### 🛠️ Hướng dẫn sử dụng

Thêm section RabbitMQ vào file `appsettings.json` của dự án:
1. Đăng ký Service

Trong file `Program.cs`, gọi hàm extension để đăng ký cả Publisher và Consumer:
```bash
using Nexus.BuildingBlocks.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Cách 1: Đăng ký với cấu hình mặc định (hỗ trợ đầy đủ Unicode)
builder.Services.AddSharedRabbitMQ(builder.Configuration);



// Cách 2: Đăng ký với tùy chỉnh JsonSerializerOptions
var customJsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.Create(
        UnicodeRanges.BasicLatin,
        UnicodeRanges.Latin1Supplement,
        UnicodeRanges.LatinExtendedA,
        UnicodeRanges.LatinExtendedB,
        UnicodeRanges.LatinExtendedAdditional 
    ),
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
};

builder.Services.AddSharedRabbitMQ(builder.Configuration, customJsonOptions)


var app = builder.Build();
```

2. Gửi tin nhắn (Publishing) 📤
   
Inject interface `IMessagePublisher` vào Controller hoặc Service của bạn. Bạn không cần lo lắng về việc mở kết nối, thư viện sẽ tự xử lý.

```bash
public class UsersController : ControllerBase
{
    private readonly IMessagePublisher _publisher;

    public UsersController(IMessagePublisher publisher)
    {
        _publisher = publisher;
    }

    [HttpPost("register")]
    public async Task<IActionResult> Register(UserRegistrationDto user)
    {
        // Cách 1: Gửi thẳng vào Queue (Tự động tạo Queue nếu chưa có)
        await _publisher.PublishAsync("user-registered-queue", user);

        // Cách 2: Gửi vào Exchange kèm Routing Key (Pub/Sub pattern)
        await _publisher.PublishAsync(
            exchange: "user.events",
            exchangeType: ExchangeType.Topic, // "direct", "fanout", "topic", "headers"
            routingKey: "user.registered",
            message: new
            {
                EventId = Guid.NewGuid(),
                EventType = "UserRegistered",
                EventTime = DateTime.UtcNow,
                UserId = user.Id,
                Email = user.Email,
                FullName = "Phúc Đại", 
                PhoneNumber = user.Phone,
                RegistrationSource = "Web"
            });

        return Ok(new { Message = "User registered successfully" });
    }
}
```

3. Nhận tin nhắn (Consuming) 📥

Để nhận tin nhắn, hãy tạo một BackgroundService.

```⚠️ QUAN TRỌNG: Luôn đặt logic đăng ký (Subscribe) bên trong hàm ExecuteAsync và sử dụng Task.Yield() hoặc để nó chạy ngầm. TUYỆT ĐỐI KHÔNG đặt trong StartAsync vì sẽ làm treo ứng dụng (block Swagger).```
```bash
using Nexus.BuildingBlocks.Interfaces;

public class UserEventsConsumer : BackgroundService
{
    private readonly IMessageConsumer _consumer;
    private readonly ILogger<UserEventsConsumer> _logger;

    public UserEventsConsumer(
        IMessageConsumer consumer,
        ILogger<UserEventsConsumer> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // Tránh block ứng dụng

        // 1. Đăng ký lắng nghe Queue đơn giản
        await _consumer.Subscribe<UserRegisteredEvent>(
            queueName: "user-registered-queue",
            handler: HandleUserRegistered);

        // 2. Đăng ký lắng nghe Exchange phức tạp
        await _consumer.Subscribe<UserRegisteredEvent>(
            exchange: "user.events",
            exchangeType: ExchangeType.Topic,
            routingKey: "user.registered",
            queueName: "auth-service-user-registered",
            handler: HandleUserRegistered);
        
        // Giữ service chạy ngầm
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task HandleUserRegistered(UserRegisteredEvent userEvent)
    {
        // Tin nhắn sẽ được deserialize với Unicode đầy đủ
        _logger.LogInformation($"Đang xử lý user: {userEvent.FullName}");
        
        // Xử lý business logic ở đây
        await SendWelcomeEmailAsync(userEvent.Email, userEvent.FullName);
        await UpdateAnalyticsAsync(userEvent.UserId);
        
        _logger.LogInformation($"Xử lý xong user: {userEvent.FullName}");
    }
    
    private async Task SendWelcomeEmailAsync(string email, string fullName)
    {
        // Gửi email chào mừng với tên Unicode
        // Ví dụ: "Chào mừng Phúc Đại đến với hệ thống!"
    }
}

// Định nghĩa DTO cho event
public class UserRegisteredEvent
{
    public string EventId { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public DateTime EventTime { get; set; }
    public string UserId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string FullName { get; set; } = string.Empty; 
    public string RegistrationSource { get; set; } = "Web";
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
builder.Services.AddHostedService<UserEventsConsumer>();
```

### Tùy chỉnh Json Serialization

Mặc định (Recommended):
Thư viện đã cấu hình sẵn với Unicode support đầy đủ và an toàn:
```bash
// Cấu hình mặc định
var defaultOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All), // Hỗ trợ tất cả Unicode
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
};
```

Tùy chỉnh theo nhu cầu:
```bash
// Tối ưu hiệu suất - chỉ hỗ trợ các Unicode ranges cần thiết
var optimizedOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Encoder = JavaScriptEncoder.Create(new[]
    {
        UnicodeRanges.BasicLatin,            // A-Z, a-z, 0-9, basic symbols
        UnicodeRanges.Latin1Supplement,      // Latin-1: ç, ñ, ß, etc.
        UnicodeRanges.LatinExtendedA,        // Latin extended: ā, ē, etc.
        UnicodeRanges.LatinExtendedB,        // More Latin
        UnicodeRanges.LatinExtendedAdditional, // Vietnamese: ắ, ằ, ẳ, etc.
        UnicodeRanges.GeneralPunctuation,    // Punctuation
        UnicodeRanges.CurrencySymbols,       // $, €, £, ¥, etc.
        UnicodeRanges.NumberForms,           // ¼, ½, ¾, etc.
        UnicodeRanges.MathematicalOperators  // +, -, ×, ÷, etc.
    }),
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
};

// Đăng ký với options tùy chỉnh
builder.Services.AddSharedRabbitMQ(builder.Configuration, optimizedOptions);
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
