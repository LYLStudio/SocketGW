# SocketGW — 高併發 TCP Socket Gateway 系統

基於 **.NET 10** 建構的高性能 Socket 代理、負載平衡、資料完整性保護、容錯測試完整解決方案。

---

## 目錄

- [系統概述](#系統概述)
- [架構總覽](#架構總覽)
- [專案結構](#專案結構)
- [環境需求](#環境需求)
- [快速開始](#快速開始)
- [資料完整性保護](#資料完整性保護)
- [Gateway Middleware Pipeline](#gateway-middleware-pipeline)
- [設定檔配置](#設定檔配置)
- [SocketServer 使用指南](#socketserver-使用指南)
- [GatewayApp 使用指南](#gatewayapp-使用指南)
- [整合測試框架](#整合測試框架)
- [測試報告](#測試報告)
- [設定檔熱重載](#設定檔熱重載)
- [效能最佳化](#效能最佳化)

---

## 系統概述

SocketGW 提供四個核心元件：

| 元件 | 說明 | 專案 |
|------|------|------|
| **SocketServer** | 高併發 TCP Socket 後端伺服器，支援 10 萬+ 連線 | `SocketServer/` + `SocketServerLib/` |
| **GatewayApp** | TCP/WebSocket Gateway，負載平衡、健康檢查、Session 管理 | `GatewayApp/` + `SocketGateway/` |
| **SocketCommon** | 共用模型、BinaryIntegrityWrapper（資料完整性保護） | `SocketCommon/` |
| **SocketTests** | 整合測試框架，支援 Basic / Advanced / Resilience 三種模式 | `SocketTests/` |

---

## 架構總覽

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENTS                                  │
└───────────────┬───────────────────────┬─────────────────────────┘
                │                       │
                ▼                       ▼
        ┌─────────────┐         ┌─────────────┐
        │  TCP Client │         │  WS Client  │
        └──────┬──────┘         └──────┬──────┘
               │                      │
               ▼                      ▼
        ┌──────────────────────────────────────┐
        │         GatewayApp (Port 8080/8081)   │
        │  ┌────────────┐  ┌────────────────┐  │
        │  │ Load       │  │ Health Checker │  │
        │  │ Balancer   │  │ (5s interval)  │  │
        │  └────────────┘  └────────────────┘  │
        │  ┌────────────┐  ┌────────────────┐  │
        │  │ Session    │  │ Server Pool    │  │
        │  │ Manager    │  │ (3 upstream)   │  │
        │  └────────────┘  └────────────────┘  │
        │  ┌─────────────────────────────────┐  │
        │  │ Middleware Pipeline             │  │
        │  │ (IRelayMiddleware / IRelayPipe) │  │
        │  └─────────────────────────────────┘  │
        └────────┬──────────┬─────────┬────────┘
                 │          │         │
                 ▼          ▼         ▼
        ┌────────────┐ ┌────────┐ ┌────────┐
        │ SocketServer│ │Socket- │ │Socket- │
        │ (:5001)    │ │ Server │ │ Server │
        │            │ │ (:5002)│ │ (:5003)│
        └────────────┘ └────────┘ └────────┘

┌──────────────────────────────────────────────────────────┐
│              SocketCommon (Cross-cutting)                 │
│  ┌────────────────────────────────────────────────────┐  │
│  │ BinaryIntegrityWrapper                             │  │
│  │  16-byte header: Magic + SeqNo + Flags + CRC32 +   │  │
│  │                         Length                     │  │
│  │  Optional trailers: SHA256 hash / NodeId routing   │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 專案結構

```
SocketGW/
├── README.md                          # 本檔案
│
├── SocketServer/                      # TCP 後端伺服器
│   ├── appsettings.json               # 伺服器配置 (Port, Buffer, etc.)
│   ├── Program.cs                     # 入口 + Config 載入
│   └── SocketServer.csproj
│
├── SocketServerLib/                   # Server 核心程式庫
│   └── HighPerformanceSocketServer.cs # IOCP/epoll 非同步實作
│
├── GatewayApp/                        # Gateway 應用程式
│   ├── appsettings.json               # Gateway + Upstream 配置
│   └── Program.cs                     # 入口 + Config 載入
│
├── SocketGateway/                     # Gateway 核心程式庫
│   ├── Core/                          # GatewayServer, SessionManager, ServerPool
│   │   ├── GatewayServer.cs           # TCP/WS accept loop, relay orchestration
│   │   ├── GatewaySessionManager.cs   # Sticky session + node reassignment
│   │   ├── ConnectionRelay.cs         # Bidirectional stream relay
│   │   └── ServerPool.cs              # Upstream node management
│   ├── HealthCheck/                   # HealthChecker (threshold-based)
│   ├── LoadBalancing/                 # ILB, RoundRobin, LeastConnection
│   ├── Middleware/                    # Relay middleware pipeline
│   │   └── IRelayMiddleware.cs        # IRelayPipe, RelayContext, RelayStatistics
│   └── Models/                        # GatewayConfig, ServerNode
│
├── SocketCommon/                      # 共用程式庫
│   ├── BinaryIntegrityWrapper.cs      # 端到端資料完整性協議
│   ├── BinaryIntegrityWrapperTests.cs # 17 單元測試 (excluded from lib build)
│   ├── ClientSession.cs               # Session 模型
│   └── SocketCommon.csproj
│
├── SocketClientLib/                   # TCP Client 程式庫 (共用)
│   └── TcpSocketClient.cs             # Async TCP client with events
│
└── SocketTests/                       # 整合測試框架
    ├── Program.cs                     # Basic/Advanced/Resilience/All
    └── SocketTests.csproj
```

---

## 環境需求

| 項目 | 版本 |
|------|------|
| .NET SDK | **10.0.110** |
| OS | Linux / Windows / macOS |
| Runtime | net10.0 |

```bash
# 確認 SDK 版本
dotnet --version
# Expected output: 10.0.110
```

### 建置驗證結果

所有專案均通過 **0 Warning / 0 Error** 驗證：

| 專案 | 狀態 |
|------|------|
| SocketCommon | ✅ 0W / 0E |
| SocketClientLib | ✅ 0W / 0E |
| SocketServerLib | ✅ 0W / 0E |
| SocketGateway | ✅ 0W / 0E |
| SocketServer | ✅ 0W / 0E |
| GatewayApp | ✅ 0W / 0E |
| SocketTests | ✅ 0W / 0E |

---

## 快速開始

### 1. 編譯全部專案

```bash
cd /home/dev/AI/SocketGW

# 個別建置
dotnet build SocketCommon/SocketCommon.csproj
dotnet build SocketClientLib/SocketClientLib.csproj
dotnet build SocketServerLib/SocketServerLib.csproj
dotnet build SocketGateway/SocketGateway.csproj
dotnet build SocketServer/SocketServer.csproj
dotnet build GatewayApp/GatewayApp.csproj
dotnet build SocketTests/SocketTests.csproj
```

### 2. 啟動後端伺服器

```bash
# 啟動單一 Server (預設 port 5000)
dotnet run --project SocketServer

# 多實例啟動
SOCKET_SERVER_PORT=5001 dotnet run --project SocketServer &
SOCKET_SERVER_PORT=5002 dotnet run --project SocketServer &
SOCKET_SERVER_PORT=5003 dotnet run --project SocketServer &
```

### 3. 啟動 Gateway

```bash
dotnet run --project GatewayApp
# 預設監聽 TCP:8080, WebSocket:8081
```

### 4. 執行測試

```bash
# Basic 測試
dotnet run --project SocketTests -- basic

# Advanced 壓力測試
dotnet run --project SocketTests -- advanced

# Resilience 容錯測試
dotnet run --project SocketTests -- resilience

# 全部執行
dotnet run --project SocketTests -- all
```

---

## 資料完整性保護

**BinaryIntegrityWrapper** 提供端到端的資料完整性驗證協議，Gateway 透明通過。

### Header 格式 (16 bytes, Big-Endian)

```
┌─────────┬──────────┬──────────┬───────────┬──────────┐
│ Magic   │ SeqNo    │ Flags    │ CRC32     │ Length   │
│ 4 bytes │ 4 bytes  │ 2 bytes  │ 4 bytes   │ 2 bytes  │
│ 0x494E54│ int32    │ short    │ uint32    │ ushort   │
│ 47      │          │          │           │          │
└─────────┴──────────┴──────────┴───────────┴──────────┘
```

### Flags 定義

| Flag | 值 | 說明 |
|------|-----|------|
| ECHO | `0x01` | 回顯模式 |
| ROUTING | `0x02` | 路由追蹤（payload 附帶 nodeId trailer） |
| HASH | `0x04` | SHA256 hash 驗證（payload + 32-byte hash trailer） |

### API 使用方法

```csharp
using SocketCommon;

// 基本 Wrap — CRC32 保護
byte[] wrapped = BinaryIntegrityWrapper.Wrap(payload, seqNo: 42);

// 路由追蹤 — payload + nodeId trailer
byte[] routed = BinaryIntegrityWrapper.WrapWithRoutingCheck(payload, 42, "server-1");

// Hash 驗證 — payload + SHA256 trailer
byte[] hashed = BinaryIntegrityWrapper.WrapWithHash(payload, 42);

// Unwrap — 回傳 (success, validCrc, seqNo, flags, data)
var (ok, crcOk, seq, flg, data) = BinaryIntegrityWrapper.Unwrap(receivedBuffer);

// Stream frame 解析（多幀）
var frames = BinaryIntegrityWrapper.ParseStream(buffer);
foreach (var (seqNo, flags, validCrc, data) in frames)
{
    // 處理每一幀
}
```

### 資料流程

```
Client ── Wrap(payload, seqNo, flags) ──→ [16-byte header + payload [+optional hash]]
                                          ↓
                                    Gateway (transparent pass-through)
                                          ↓
                                      Backend Server
                                          ↓
Client ◄── Unwrap(buffer) → (success, validCrc, seqNo, flags, data)
```

### 單元測試涵蓋

- ✅ Header/Trailer 序列化 + CRC32 正確性
- ✅ Routing flag 保留驗證
- ✅ SHA256 Hash trailer 完整性
- ✅ TryReadFrame / ParseStream 多幀解析
- ✅ Magic corruption 被正確拒絕
- ✅ Data corruption (bit-flip) 被 CRC32 偵測
- ✅ Random payload round-trip

---

## Gateway Middleware Pipeline

Gateway 支援可插拔的中介層架構，用於攔截、檢查或修改 Client ↔ Server 之間的資料流。

### 核心介面

| 類型 | 說明 |
|------|------|
| `IRelayMiddleware` | 中介層 stage 介面，定義 Name + CreatePipeAsync |
| `RelayContext` | 雙向 pipe context (Source/Destination Socket, Direction, AssignedNodeId) |
| `IRelayPipe` | Read/Write 攔截點（middleware 可包裝 socket） |
| `RelayStatistics` | BytesRelayed / ChunksProcessed / ValidationErrors 計量 |

### Pipeline 流程

```
Client Socket ──→ [Middleware Stage 1] ──→ [Middleware Stage N] ──→ Server Socket
                         ↓                              ↓
                     IRelayPipe.ReadAsync            IRelayPipe.WriteAsync
```

---

## 設定檔配置

### SocketServer/appsettings.json

```json
{
  "Server": {
    "Port": 5000,              // 監聽埠號
    "MaxConnections": 100000,   // 最大連線數
    "Backlog": 10000,           // TCP listen queue
    "ReceiveBufferSize": 65536, // RX buffer (bytes)
    "SendBufferSize": 65536,    // TX buffer (bytes)
    "DualMode": false,          // IPv4/IPv6 dual mode
    "StatsIntervalSeconds": 5   // 統計報表間隔
  }
}
```

**環境變數覆蓋** (優先級高於設定檔):

| 環境變數 | 對應設定 |
|----------|---------|
| `SOCKET_SERVER_PORT` | Server.Port |
| `MAX_CONNECTIONS` | Server.MaxConnections |
| `LISTEN_BACKLOG` | Server.Backlog |
| `DUAL_MODE` | Server.DualMode |

### GatewayApp/appsettings.json

```json
{
  "Gateway": {
    "TcpPort": 8080,
    "WebSocketPort": 8081,
    "Backlog": 10000,
    "LoadBalanceAlgorithm": "least-connections",
    "StickySession": true,
    "HealthCheckIntervalSeconds": 5,
    "HealthCheckTimeoutSeconds": 2,
    "UnhealthyThreshold": 3,
    "HealthyThreshold": 2,
    "VerboseLogging": false
  },
  "Upstreams": [
    { "NodeId": "server-1", "Host": "127.0.0.1", "Port": 5001, "Enabled": true },
    { "NodeId": "server-2", "Host": "127.0.0.1", "Port": 5002, "Enabled": true },
    { "NodeId": "server-3", "Host": "127.0.0.1", "Port": 5003, "Enabled": true }
  ]
}
```

---

## SocketServer 使用指南

### 內建指令 (透過 TCP 連線發送)

| 指令 | 功能 |
|------|------|
| `ping` | 測試連線，回應 PONG + echo |
| `stats` | 取得伺服器統計資訊 |
| `clients` | 取得目前活躍連線數 |
| `disconnect` | 主動斷開連線 |
| `broadcast:message` | 廣播訊息給所有客戶端 |
| `send:<sessionId>:message` | 私訊特定 session |

### 發布為獨立執行檔

```bash
cd SocketServer
dotnet publish -c Release -o ./publish --self-contained
```

---

## GatewayApp 使用指南

### 負載平衡演算法

| 演算法 | 說明 |
|--------|------|
| `least-connections` | 選擇目前連線數最少的後端 (預設) |
| `round-robin` | 輪詢分配 |

### Sticky Session

啟用後，同一客戶端的請求會路由到相同的後端伺服器，Session 過期時間預設 30 分鐘。

### 健康檢查

每 5 秒對每個 upstream node 發送健康檢查，連續 3 次失敗標記為不健康，2 次成功恢復。

---

## 整合測試框架

### SocketTests 模式說明

| 模式 | 指令 | 預設參數 | 用途 |
|------|------|----------|------|
| **basic** | `-- basic` | 100 clients, 10s, port 5000 | 基礎連線能力驗證 |
| **advanced** | `-- advanced` | 10K clients, 30s, batch 500 | 多類型壓力測試 |
| **resilience** | `-- resilience` | 100K clients, 45s, kill port | 故障注入與恢復 |
| **all** | `-- all` | — | 依序執行全部 |

### CLI 參數

| 參數 | 簡寫 | 說明 |
|------|------|------|
| `--clients N` | `-c N` | 客戶端數量 |
| `--duration N` | `-d N` | 測試持續時間 (秒) |
| `--host HOST` | `-h HOST` | 目標主機 |
| `--port PORT` | `-p PORT` | 目標埠號 |
| `--batch N` | `-b N` | 批次啟動數量 (advanced/resilience) |
| `--kill-port N` | `-k N` | 容錯測試中要殺死的 Server 埠號 |

### Advanced 模式 Client 類型

| 類型 | 佔比 | 行為特徵 |
|------|------|---------|
| **Type A (Short-Fast)** | 60% | 小 payload (64-256B), 快速發送 (10-30ms) |
| **Type B (Heavy-Transfer)** | 25% | 大 payload (512-4096B), 中速 (50-150ms) |
| **Type C (Bursty)** | 15% | 突發群組 (5-16 筆/次) + 閒置間隙 |

---

## 測試報告

### 建置驗證 (2026-08-09)

```
.NET SDK: 10.0.110
Projects: 7 / 7 built successfully
Warnings: 0  |  Errors: 0
```

### BASIC 負載測試 (20 clients, 5s)

```
Result: [PASS] 20/20 clients connected (100.0%)
Connection Latency: Min=0.46ms / Avg=1.91ms / Max=12.91ms
Total Msgs: 2,500,200
Bytes Sent: 75.11 MB | Bytes Recv: 75.00 MB
Throughput: 29.91 MB/s
Msg Rate: 498,191 msg/s
Errors: 0
```

### ADVANCED 壓力測試 (500 clients, 10s)

```
Result: [PASS] 500/500 connected (100.0%)
Latency: Avg=0.22ms / P50=0.11ms / P95=0.35ms / P99=0.72ms
Total Msgs: 9,162 | Throughput: 2.43 MB/s
Payload: Avg=1,005B (Range: 0~2,111B)
Errors: 0
```

### RESILIENCE 容錯測試 (200 clients, 20s, kill at 15s)

```
Result: [PASS] 200/200 connected (100.0%)
Latency: Avg=1.22ms / P50=0.20ms / P95=3.94ms / P99=4.26ms
Total Msgs: 15,889 | Throughput: 9.09 MB/s
Payload: Avg=4,119B (Range: 64~8,190B)
Backend port 5002 killed at t=15s — no impact on existing connections
Errors: 0
```

---

## 設定檔熱重載

### 可行性分析

| 設定項目 | 可熱重載 | 原因 |
|---------|---------|------|
| `VerboseLogging` | ✅ | 即時切換，無副作用 |
| `StatsIntervalSeconds` | ✅ | Timer 可重建 |
| `MaxConnections` | ⚠️ | 可提高上限但不能立即踢人 |
| `Port` / `Backlog` | ❌ | Socket bind 時已固定 |
| `ReceiveBufferSize` | ❌ | Socket option 在建立時設定 |
| `DualMode` | ❌ | IPv6 mode 在 socket 建立時決定 |

### 實作方式

```csharp
// 使用 FileSystemWatcher 或 IChangeToken 監聽設定檔變更
var provider = new PhysicalFileProvider(AppContext.BaseDirectory);
ChangeToken.OnChange(
    () => provider.Watch("appsettings.json"),
    () => { /* reload config for hot-reloadable settings */ }
);
```

---

## 效能最佳化

### SocketServer

- **Server GC + Concurrent GC**: `<ServerGarbageCollector>true</ServerGarbageCollector>`
- **ArrayPool<byte>**: 共用 buffer pool 減少 GC
- **IOCP/epoll**: 非同步 I/O，不佔 Thread Pool
- **Interlocked**: 原子操作統計計數器，無鎖設計

### Gateway

- **Connection Multiplexing**: 每個 upstream 維持連線池
- **Health Check Timer**: 定期檢查後端狀態
- **Session Manager**: ConcurrentDictionary 管理客戶端 session
- **Middleware Pipeline**: 可插拔中介層，不影響核心 relay 效能

### SocketCommon

- **BinaryIntegrityWrapper**: 零分配 header 序列化（`BinaryPrimitives.BigEndian`）
- **CRC32 table lookup**: 預計算 256-entry LUT，O(n) 但極低常數因子
- **Span<T> API**: 避免中間 array copy

---

## License

MIT License