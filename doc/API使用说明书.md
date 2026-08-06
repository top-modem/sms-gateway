# SMS Gateway HTTP API 使用说明书

本文档列出 SMS Gateway 项目暴露的所有 REST API 接口、请求/响应格式、认证方式以及常见使用示例，并配套提供可直接在 Postman 中导入的测试集合。

---

## 1. 概述

- **框架**: Rust + Axum 0.8
- **基础地址**: `http://<server_host>:<server_port>`，默认 `http://127.0.0.1:8080`
- **API 根路径**: 所有接口均挂载在 `/api` 下
- **前端静态资源**: 非 `/api` 路径会返回 `frontend/dist` 中的 SPA 资源
- **认证方式**: 所有 `/api` 接口均使用 **Basic Auth**；用户名/密码读取自 `config.toml`
  - 默认用户名: `admin`
  - 默认密码: `123456`
  - HTTP 请求头: `Authorization: Basic <base64(username:password)>`，例如 `Basic YWRtaW46MTIzNDU2`

---

## 2. 通用约定

### 2.1 状态码

| 状态码 | 含义 |
|--------|------|
| 200    | 成功 |
| 202    | 已接受（异步任务） |
| 204    | 成功但无返回内容（如 `/api/check`） |
| 400    | 请求参数错误 |
| 401    | 未认证 / Basic Auth 失败 |
| 404    | 资源不存在 |
| 500    | 服务器内部错误 |
| 502    | 调制解调器（Modem）操作失败 |

### 2.2 通用数据类型

| 类型 | 说明 |
|------|------|
| `SmsStatus` | `0=未读`, `1=已读`, `2=发送中`, `3=失败` |
| `SmsStorage` | `"SIM"`, `"ME"`, `"MT"` |
| `PhoneResultStatus` | `"Success"`, `"Skipped"`, `"Failed"` |

---

## 3. 接口分组

### 3.1 健康检查与服务控制

#### GET `/api/check`
验证 Basic Auth 是否有效，无返回体。

**响应**: `204 No Content`

---

#### GET `/api/diagnostics`
获取服务器运行诊断信息。

**响应示例**:
```json
{
  "server_start_time": "2026-07-16T08:00:00",
  "uptime_seconds": 3600,
  "service_running": true,
  "modem_count": 4,
  "modems": [
    { "sim_id": "8901234567890123456", "com_port": "COM3", "model": "SIM7600" }
  ],
  "sim_card_count": 4,
  "sim_cards": [...],
  "platform_batch_upload_count": 10,
  "platform_item_count": 5,
  "pending_sms_upload_count": 2
}
```

---

#### GET `/api/service/status`
获取服务运行状态。

**响应**:
```json
{ "running": true }
```

---

#### POST `/api/service/start`
启动内部服务运行标志。

**响应**:
```json
{ "running": true }
```

---

#### POST `/api/service/stop`
停止内部服务运行标志。

**响应**:
```json
{ "running": false }
```

---

### 3.2 SMS 短信

#### GET `/api/sms`
分页查询短信记录。

**查询参数**:
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| page | u32 | 否 | 页码，默认 1 |
| per_page | u32 | 否 | 每页条数，默认 20 |
| contact_id | String | 否 | 按联系人 ID 过滤 |
| direction | String | 否 | `inbox` 或 `sent` |

**响应示例**:
```json
{
  "data": [
    {
      "id": 1,
      "contact_id": "+8618126101015",
      "timestamp": "2026-07-16T08:30:00",
      "message": "hello",
      "sim_id": "8901234567890123456",
      "send": true,
      "status": 1,
      "uploaded_to_platform": false,
      "platform_item_id": null,
      "platform_uploaded_at": null,
      "platform_response": null
    }
  ],
  "total": 100,
  "page": 1,
  "per_page": 20
}
```

当传入 `direction` 时，`data` 中每一项额外包含 `contact_name`。

---

#### POST `/api/sms`
发送短信。`sim_id` 与 `phone_number` 至少提供一个。

**请求体**:
```json
{
  "sim_id": "8901234567890123456",
  "phone_number": "+8618126101015",
  "contact": { "id": "+8618126101015", "name": "Test" },
  "message": "hello from postman",
  "new": true
}
```

**响应成功**:
```json
{ "sms_id": 123, "contact_id": "+8618126101015" }
```

**错误**:
- 400: 未提供 `sim_id` 或 `phone_number`
- 404: 找不到 SIM
- 500: 发送失败

---

#### GET `/api/sms/sse`
短信/会话实时 Server-Sent Events 流。

**请求头**: `Accept: text/event-stream`

**事件类型**: `conversations`

---

### 3.3 SIM 与 AT 指令

#### GET `/api/sims/info`
获取所有调制解调器（SIM）的运行时信息，包括信号、网络、运营商、型号等。

**响应**: 调制解调器对象数组。

---

#### GET `/api/sims/refresh-all`
刷新内存中的 SIM 缓存。

**响应**:
```json
{ "refreshed": 4, "sim_ids": ["8901...", ...] }
```

---

#### GET `/api/sims/{sim_id}/refresh`
从指定 SIM 读取未读短信并写入数据库。

**响应**: `200` 空体；失败返回 `502` 及错误文本。

---

#### POST `/api/at/{com_port}`
向指定串口发送原始 AT 指令。

**请求体**:
```json
{ "command": "AT+CSQ" }
```

**响应成功**:
```json
{ "response": "+CSQ: 20,0\r\n\r\nOK\r\n" }
```

**响应失败**:
```json
{ "error": "Modem not found on port: COM99" }
```

---

#### GET `/api/sim-cards`
获取数据库中所有 SIM 卡记录。

**响应**: `[SimCard]`

```json
[
  {
    "id": "8901234567890123456",
    "imsi": "460001234567890",
    "phone_number": "+8618126101015",
    "alias": "Office SIM",
    "country_code": "CN",
    "created_at": "2026-01-01T00:00:00",
    "updated_at": "2026-07-16T08:00:00"
  }
]
```

---

#### GET `/api/sims/stats`
按 SIM 统计收发短信数量。

**响应**:
```json
[
  { "sim_id": "8901...", "recv": 10, "sent": 5, "phone_number": "+8618126101015" }
]
```

---

#### GET `/api/sims/{sim_id}/info`
获取指定 SIM 的增强信息。

---

#### PUT `/api/sim-cards/{sim_id}/alias`
修改 SIM 卡别名。

**请求体**:
```json
{ "alias": "Office SIM" }
```

---

#### PUT `/api/sim-cards/{sim_id}/phone`
仅修改数据库中 SIM 卡手机号（不写入 SIM）。

**请求体**:
```json
{ "phone_number": "+8618126101015" }
```

---

#### POST `/api/sims/{sim_id}/phone`
通过 AT 指令将手机号写入 SIM 并持久化到数据库。

**请求体**:
```json
{ "phone_number": "+8618126101015" }
```

---

#### GET `/api/sims/{sim_id}/storage`
获取 SIM 短信存储状态。

**响应**:
```json
{ "status": "SIM: 5/50, ME: 0/100" }
```

---

#### PUT `/api/sims/{sim_id}/storage`
设置 SIM 短信存储位置。

**请求体**:
```json
{ "storage": "SIM" }
```

可选值: `"SIM"`, `"ME"`, `"MT"`。

---

### 3.4 联系人与会话

#### GET `/api/contacts`
获取所有联系人。

**响应**:
```json
[
  { "id": "+8618126101015", "name": "Test" }
]
```

---

#### POST `/api/contacts`
创建联系人。

**请求体**:
```json
{ "id": "+8618126101015", "name": "Test Contact" }
```

---

#### DELETE `/api/contacts/{id}`
删除联系人，同时删除该联系人的所有短信。

---

#### GET `/api/conversation`
获取会话列表。

---

#### POST `/api/conversations/{id}/unread`
获取指定联系人的未读短信，并将其标记为已读。

**路径参数**: `id` 为 `contact_id`。

**响应**: `[Sms]`

---

### 3.5 电话号码管理

#### POST `/api/phone-numbers/import`
批量导入 ICCID/MSISDN 到 SIM 卡。

**请求体**:
```json
{
  "entries": [
    { "iccid": "8944010000000000001", "msisdn": "447700000001" },
    { "iccid": "8944010000000000002", "msisdn": "447700000002" }
  ]
}
```

**响应**: `202 Accepted`
```json
{ "status": "started" }
```

---

#### POST `/api/phone-numbers/barcode-scan`
扫描条码记录 ICCID/MSISDN。

**请求体**:
```json
{ "iccid": "8944010000000000001", "msisdn": "447700000001" }
```

**校验规则**: ICCID 必须以 `8944` 开头且 20 位；MSISDN 为 11 位英国号段。

---

#### GET `/api/phone-numbers/barcode-scans`
获取未导入的条码扫描记录。

---

#### DELETE `/api/phone-numbers/barcode-scans`
清空未导入的扫描记录。

**响应**:
```json
{ "status": "cleared" }
```

---

#### POST `/api/phone-numbers/barcode-scans/import`
将扫描到的 MSISDN 写入 SIM 卡。

**响应**:
```json
{ "imported_count": 5 }
```

---

#### POST `/api/phone-numbers/call-exchange`
通过互拨发现手机号。

**响应**:
```json
{ "status": "started" }
```

---

#### POST `/api/phone-numbers/sms-exchange`
通过互发短信发现手机号。

**响应**:
```json
{ "status": "started" }
```

---

#### POST `/api/phone-numbers/ussd`
向所有无手机号的 SIM 发送 USSD。

**请求体**:
```json
{ "code": "*#100#" }
```

---

#### GET `/api/phone-numbers/status`
获取号码发现任务进度。

**响应**: `PhoneNumberTask`

```json
{
  "running": true,
  "task_type": "call-exchange",
  "total": 10,
  "done": 4,
  "current": "COM3",
  "errors": [],
  "results": [
    { "com_port": "COM3", "sim_id": "8901...", "phone_number": "+447700000001", "status": "Success", "message": "" }
  ]
}
```

---

### 3.6 火狐狸（Firefox）平台集成

#### GET `/api/settings/firefox-api-key`
获取已保存的火狐狸 API Key。

**响应**:
```json
{ "api_key": "your-api-key" }
```

---

#### PUT `/api/settings/firefox-api-key`
保存火狐狸 API Key。

**请求体**:
```json
{ "api_key": "your-api-key" }
```

---

#### GET `/api/firefox/countries`
获取支持的国家/地区列表。

**响应**:
```json
[
  { "id": "eng", "prefix": "44", "name": "United Kingdom" }
]
```

---

#### POST `/api/firefox/upload`
将本地 SIM 手机号上传到火狐狸平台。

**请求体**:
```json
{
  "sim_ids": ["8901234567890123456"],
  "country_id": "eng"
}
```

**响应**:
```json
{
  "message": "Upload completed",
  "uploaded_count": 10,
  "batch_ids": ["batch-123"],
  "results": [{ "code": "0", "data": "ok" }]
}
```

---

#### POST `/api/firefox/batch-status`
查询批量任务状态。

**请求体**:
```json
{ "batch_id": "batch-123" }
```

---

#### GET `/api/firefox/batch-uploads`
获取本地批量上传历史（最近 100 条）。

---

#### POST `/api/firefox/delete-batch`
按手机号批量删除平台号码。

**请求体**:
```json
{
  "entries": [
    { "country_id": "eng", "phone_num": "447700000001" },
    { "country_id": "eng", "phone_num": "447700000002" }
  ]
}
```

---

#### POST `/api/firefox/delete-batch-by-id`
按本地 batch_id 删除平台号码。

**请求体**:
```json
{ "batch_id": "batch-123" }
```

---

#### POST `/api/firefox/delete-country`
按国家删除平台号码。

**请求体**:
```json
{ "country_id": "eng" }
```

---

#### POST `/api/firefox/delete-all`
清空本地所有国家码及批量上传历史。

---

#### GET `/api/firefox/wait-list`
获取平台待处理列表。

---

#### GET `/api/firefox/result-list`
查询平台结果列表。

**查询参数**: `country_id`, `phone_num`, `item_id`

---

#### POST `/api/firefox/upload-sms`
手动上传单条短信到平台。

**请求体**:
```json
{
  "country_id": "eng",
  "phone_num": "447700000001",
  "sms_content": "Your code is 123456"
}
```

---

#### GET `/api/firefox/platform-items`
获取平台项目（Platform Item）列表。

---

#### GET `/api/firefox/platform-items/{item_id}`
获取指定项目详情及关联短信列表。

**查询参数**: `sim_id`（可选）

---

#### GET `/api/firefox/platform-statistics`
获取平台项目统计（按项目和 SIM 聚合）。

**响应**:
```json
[
  {
    "item_id": "1001",
    "item_name": "Instagram",
    "country_id": "eng",
    "phone_num": "447700000001",
    "iccid": "8901...",
    "total_sms": 10,
    "uploaded_sms": 8,
    "failed_sms": 2
  }
]
```

---

#### GET `/api/firefox/platform-rejection-reasons`
获取平台 rejection reason 统计 Top 列表。

**响应**:
```json
[
  { "reason": "Phone number already exists", "count": 5 }
]
```

---
### 3.6.1 大厅计费/收益（Money）

#### GET `/api/firefox/money-stats`
按串口统计每张 SIM 的等待/接收/成功上传/失败短信数与收益金额。

**响应示例**:
```json
[
  {
    "com_port": "COM3",
    "phone_number": "+447700000001",
    "sim_id": "8901...",
    "imsi": "234100123456789",
    "country_code": "eng",
    "platform_connected": true,
    "waiting_sms_count": 1,
    "received_sms_count": 20,
    "successful_uploaded_sms_count": 18,
    "failed_sms_count": 2,
    "money_earning": 9.0,
    "earning_item_names": "WhatsApp|Instagram"
  }
]
```

---

#### GET `/api/firefox/money-items`
按关键字搜索平台项目（用于「项目」下拉框）。

**查询参数**: `keyword`（可选）、`limit`（可选，默认 200，最大 1000）

---

#### GET `/api/firefox/money-item-earning`
获取指定项目的成功接码数量。

**查询参数**: `item_id`

**响应示例**:
```json
{ "item_id": "1001", "success_count": 42 }
```

---

#### GET `/api/firefox/money-item-platform-prices`
获取指定项目在各国家的大厅参考单价列表。

**查询参数**: `item_id`

**响应示例**:
```json
[
  { "country_id": "eng", "country_title": "+44/英格兰/england", "item_uprice": 0.5 }
]
```

---

#### POST `/api/firefox/money-item-price`
更新指定项目的本地单价（用于收益计算，不影响平台真实单价）。

**请求体**:
```json
{ "item_id": "1001", "item_uprice": 0.5 }
```

---

#### GET `/api/firefox/money-sms-detail`
查询指定 SIM / 项目下的短信明细（用于对账）。

**查询参数**: `sim_id`（可选）、`item_id`（可选）

---
### 3.7 上传重试队列

#### GET `/api/firefox/upload-retry/stats`
获取重试队列统计。

**响应**:
```json
{ "total_items": 10, "ready_for_retry": 3, "dead_letter_items": 1 }
```

---

#### GET `/api/firefox/upload-retry/queue`
获取待重试项目列表。

---

#### GET `/api/firefox/upload-retry/dead-letter`
获取已超出最大重试次数的项目列表。

---

#### POST `/api/firefox/upload-retry/{id}/retry`
立即手动重试指定项目。

**响应**:
```json
{ "message": "Item scheduled for immediate retry" }
```

---

#### DELETE `/api/firefox/upload-retry/{id}`
从重试队列中删除指定项目。

**响应**:
```json
{ "message": "Item deleted from retry queue" }
```

---

### 3.8 语音通话

#### GET `/api/calls`
分页查询通话记录。

**查询参数**: `sim_id`, `limit`, `offset`

**响应**:
```json
{
  "data": [
    { "id": "call-1", "sim_id": "8901...", "phone": "+8618126101015", "direction": "outbound", "status": "ended" }
  ],
  "total": 10
}
```

---

#### GET `/api/calls/sse`
通话事件实时 SSE 流。

---

#### POST `/api/calls/make`
拨打电话。

**请求体**:
```json
{ "sim_id": "8901...", "phone": "+8618126101015" }
```

---

#### POST `/api/calls/answer`
接听来电。

**请求体**:
```json
{ "sim_id": "8901..." }
```

---

#### POST `/api/calls/hangup`
挂断通话。

**请求体**:
```json
{ "sim_id": "8901..." }
```

---

#### GET `/api/calls/{id}/recording`
下载通话录音（AMR 格式）。

**响应**: `Content-Type: audio/amr`

---

#### GET `/api/calls/{id}/transcript`
获取通话转写文本。

**响应**: `Content-Type: text/plain; charset=utf-8`

---

### 3.9 MMS 彩信

#### POST `/api/mms`
发送彩信。

**请求体**:
```json
{
  "sim_id": "8901...",
  "to": "+8618126101015",
  "subject": "Hello",
  "attachments": [
    {
      "filename": "image.jpg",
      "content_type": "image/jpeg",
      "base64": "/9j/4AAQ..."
    }
  ]
}
```

**响应**: `202 Accepted`
```json
{ "id": "mms-123", "status": "queued" }
```

---

#### GET `/api/mms`
分页查询已发送彩信。

---

#### GET `/api/mms/{id}`
获取彩信详情。

---

#### GET `/api/sim-cards/{sim_id}/mms-profile`
获取 SIM 的 MMS 配置文件（APN/MMSC/代理）。

---

#### PUT `/api/sim-cards/{sim_id}/mms-profile`
设置 SIM 的 MMS 配置文件。

**请求体**:
```json
{
  "apn": "cmwap",
  "mmsc": "http://mmsc.example.com",
  "proxy_host": "10.0.0.172",
  "proxy_port": 80
}
```

---

#### GET `/api/mms/inbox`
分页查询接收到的彩信通知。

---

#### GET `/api/mms/inbox/{id}`
获取彩信通知详情。

---

#### GET `/api/mms/inbox/{id}/parts/{part_id}`
获取彩信内容部件（图片/音频等二进制）。

---

### 3.10 eSIM 管理

> 仅当 `config.toml` 中 `esim_enabled = true` 时启用；依赖 `lpac` 命令行工具与读卡器/PCSC。

#### GET `/api/esim/ports`
列出可用于 eSIM 操作的串口/读卡器。

**响应**: `{ "success": true, "data": [PortInfo] }`

---

#### POST `/api/esim/{com}/session/enter` / `/session/exit`
进入/退出指定端口的 eSIM(LPA) 会话模式。

---

#### POST `/api/esim/{com}/reset`
重置指定端口的 eSIM 会话。

---

#### GET `/api/esim/{com}/chip`
获取 eUICC 芯片信息（EID 等）。

---

#### GET `/api/esim/{com}/profiles`
获取该端口下已安装的 eSIM Profile 列表。

---

#### POST `/api/esim/{com}/profiles/download`
下载（写入）新的 eSIM Profile。

**请求体**:
```json
{ "activation_code": "LPA:1$smdp.example.com$MATCHING-ID", "confirmation_code": "" }
```

---

#### POST `/api/esim/{com}/profiles/enable` / `/profiles/disable`
启用/停用指定 ICCID 的 Profile。

**请求体**:
```json
{ "iccid": "8944...", "refresh_flag": true }
```

---

#### POST `/api/esim/{com}/profiles/delete`
删除指定 ICCID 的 Profile。

**请求体**:
```json
{ "iccid": "8944..." }
```

---

#### POST `/api/esim/{com}/profiles/nickname`
设置 Profile 昵称。

**请求体**:
```json
{ "iccid": "8944...", "nickname": "My Profile" }
```

---

#### GET `/api/esim/{com}/notifications`
获取待处理的 eSIM 通知（GSMA 合规通知）。

---

#### POST `/api/esim/{com}/notifications/process`
处理（发送）指定序号的通知，可选是否同时删除。

**请求体**:
```json
{ "seq": "1", "remove": true }
```

---

#### GET `/api/esim/sources`
扫描配置目录中的二维码图片/文本，返回可用的激活码列表。

---

#### POST `/api/esim/sources/upload`
上传二维码图片或文本文件（`multipart/form-data`），解析出激活码。

---

#### POST `/api/esim/batch`
对多个端口批量执行下载+启用流程。

**请求体**:
```json
{ "ports": ["COM3", "COM4"], "activation_code": "LPA:1$smdp.example.com$MATCHING-ID" }
```

**响应**: `{ "success": true, "data": { "job_id": "..." } }`

---

#### GET `/api/esim/batch`
查询当前批量任务的实时进度快照。

---

## 4. 数据模型附录

### Sms
```json
{
  "id": 1,
  "contact_id": "+8618126101015",
  "timestamp": "2026-07-16T08:30:00",
  "message": "hello",
  "sim_id": "8901234567890123456",
  "send": true,
  "status": 1,
  "uploaded_to_platform": false,
  "platform_item_id": null,
  "platform_uploaded_at": null,
  "platform_response": null
}
```

### Contact
```json
{ "id": "+8618126101015", "name": "Test" }
```

### SimCard
```json
{
  "id": "8901234567890123456",
  "imsi": "460001234567890",
  "phone_number": "+8618126101015",
  "alias": "Office",
  "country_code": "CN",
  "created_at": "2026-01-01T00:00:00",
  "updated_at": "2026-07-16T08:00:00"
}
```

### Call
```json
{
  "id": "call-1",
  "sim_id": "8901...",
  "phone": "+8618126101015",
  "direction": "outbound",
  "status": "ended",
  "started_at": "2026-07-16T08:00:00",
  "ended_at": "2026-07-16T08:01:00"
}
```

### FirefoxUploadRetryItem
```json
{
  "id": 1,
  "sms_id": 123,
  "phone_number": "447700000001",
  "country_id": "eng",
  "message": "Your code is 123456",
  "retry_count": 1,
  "max_retries": 5,
  "next_retry_at": "2026-07-16T08:05:00",
  "last_error": "timeout",
  "last_response_code": "502",
  "created_at": "2026-07-16T08:00:00",
  "updated_at": "2026-07-16T08:01:00"
}
```

---

## 5. 使用 Postman 测试

项目提供完整 Postman 集合（两处路径内容一致，任选其一导入即可）：
- `doc/postman/sms-gateway.postman_collection.json`
- `postman/sms-gateway.postman_collection.json`

集合按功能分为 11 个文件夹：健康检查、短信、SIM 与 AT 指令、联系人与会话、电话号码管理（含条码扫描）、火狐狸平台集成、火狐狸上传重试队列、语音通话、MMS 彩信、大厅计费/收益、eSIM。

### 导入步骤
1. 打开 Postman → `File` → `Import`
2. 选择上述任一 JSON 文件
3. 在集合变量（Collection → Variables）中修改 `baseUrl`、`username`、`password`、`sim_id`、`phone`、`item_id`、`com` 等为你自己的值
4. 集合已在根级别配置 `Basic Auth`（引用 `{{username}}`/`{{password}}` 变量），子请求会自动继承，无需逐个添加 `Authorization` 头

### 变量说明
| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `baseUrl` | `http://127.0.0.1:8080` | 网关服务地址 |
| `username` | `admin` | Basic Auth 用户名 |
| `password` | `123456` | Basic Auth 密码 |
| `sim_id` | `REPLACE_WITH_SIM_ID` | 你的 SIM ICCID |
| `phone` | `18126101015` | 测试目标手机号 |
| `contact_id` | `REPLACE_WITH_CONTACT_ID` | 联系人 ID |
| `call_id` | `REPLACE_WITH_CALL_ID` | 通话记录 ID |
| `com_port` | `COM3` | 串口名 |
| `command` | `AT+CSQ` | AT 指令 |
| `batch_id` | `REPLACE_WITH_BATCH_ID` | 火狐狸 batch ID |
| `country_id` | `eng` | 火狐狸国家 ID |
| `firefox_api_key` | `REPLACE_WITH_FIREFOX_API_KEY` | 火狐狸平台 API Key |
| `mms_id` | `REPLACE_WITH_MMS_ID` | MMS 任务/通知 ID |
| `retry_id` | `REPLACE_WITH_RETRY_ID` | 上传重试队列条目 ID |
| `item_id` | `REPLACE_WITH_ITEM_ID` | 火狐狸平台项目 ID（大厅计费用） |
| `com` | `COM3` | eSIM 操作的串口名 |
| `iccid` | `REPLACE_WITH_ICCID` | eSIM Profile 的 ICCID |
| `activation_code` | `REPLACE_WITH_ACTIVATION_CODE` | eSIM 激活码（LPA 字符串） |

---

## 6. 注意事项

1. 所有 `/api` 接口都必须携带 Basic Auth 头，否则返回 `401`。
2. 发送短信前请确保目标 SIM 已在线且服务状态为 `running`。
3. 火狐狸平台接口依赖 API Key，调用前请先配置 `/api/settings/firefox-api-key`。
4. SSE 接口在 Postman v10+ 中支持 Stream 视图；也可使用浏览器 `EventSource` 或 `curl -N` 测试。
5. 二进制接口（录音、彩信附件）返回原始字节，Postman 中可选择 "Send and Download" 保存文件。
