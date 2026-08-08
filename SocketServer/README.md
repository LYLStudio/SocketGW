# High-Performance Socket Server

一個基於 .NET 10 建構的高併發 TCP Socket Server，採用非同步 I/O 模型（IOCP/epoll），支援數十萬並行連線。

## 系統需求

| 項目 | 版本 |
|------|------|
| .NET SDK | 10.0.x |
| OS | Linux / Windows / macOS |

## 技術特色

### 高併發架構
- **非同步 I/O 模型**: 使用 `SocketAsyncOperation` (Windows IOCP / Linux epoll)，不佔用 Thread Pool thread
- **Accept 循環**: 獨立 accept loop 確保新連線不被 blocking
- **ArrayPool<byte>**: 使用共用的 buffer pool，減少 GC 壓力

### 記憶體最佳化
- **Server GC**: 啟用 `ServerGarbageCollector` + `ConcurrentGarbageCollector`
- **ArrayPool**: 讀寫 buffer 從 `ArrayPool<byte>.Shared` 租借與歸還
- **Interlocked**: 所有統計计数器使用原子操作，無鎖設計

### 連線管理
- **ConcurrentDictionary**: Thread-safe 的 session 存取
- **CancellationTokenSource**: 每個 session 獨立的取消令牌
- **最大連線數限制**: 可配置的 `MaxConnections` 保護伺服器資源

## 環境變數配置

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `SOCKET_SERVER_PORT` | `5000` | 監聽埠號 |
| `MAX_CONNECTIONS` | `100000` | 最大並行連線數 |
| `LISTEN_BACKLOG` | `10000` | TCP listen queue 大小 |
| `DUAL_MODE` | `false` | 啟用 IPv4/IPv6 dual mode |

## 內建指令

連接至伺服器後，可發送以下文字指令：

| 指令 | 功能 |
|------|------|
| `ping` | 測試連線，回應 PONG + echo |
| `stats` | 取得伺服器統計資訊 |
| `clients` | 取得目前活躍連線數 |
| `disconnect` | 主動斷開連線 |
| `broadcast:message` | 廣播訊息給所有連線客戶端 |
| `send:<sessionId>:message` | 私訊特定 session |

## 快速開始

```bash
# 編譯
cd SocketServer
dotnet build

# 執行 (使用預設 Port 5000)
dotnet run

# 自訂埠號
SOCKET_SERVER_PORT=8080 dotnet run

# 發布為獨立執行檔
dotnet publish -c Release -o ./publish
```

## 專案結構

```
SocketServer/
├── SocketServer.csproj      # 專案設定 (含 GC 最佳化)
├── Program.cs               # 程式進入點、配置載入、統計報表
├── Models/
│   └── ClientSession.cs     # Session 模型 + Message 定義
└── Services/
    └── HighPerformanceSocketServer.cs  # Socket Server 核心實作
```

## 架構設計圖

```
                    ┌─────────────┐
   TCP Port 5000 ──▶│  AcceptLoop │ (非同步 accept)
                    └──────┬──────┘
                           │
                     ┌─────▼──────┐
                     │ New Client │
                     └─────┬──────┘
                           │
              ┌────────────▼────────────┐
         ┌───▶│ HandleClientAsync      │◀───┐
         │    │  (per-session loop)     │    │
         │    │                        │    │
         │    │  ArrayPool buffer       │    │
         │    │  ReceiveAsync(...)      │    │
         │    │  ProcessIncomingData()  │    │
         │    └─────────────────────────┘    │
         │                                   │
         └─────────── ConcurrentDictionary ───┘
                    <string, ClientSession>
```

## License

MIT License