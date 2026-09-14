# agent.md

本文件是本仓库的编码代理工作规则。修改代码前先阅读本文件，并以当前代码、测试和配置为准；不要把被忽略的本地运行产物当作可提交源码。

## 开发命令

### 后端 (Go)
```bash
go run main.go start                # 启动服务 (默认 0.0.0.0:8080)
go run main.go start --config path  # 指定配置文件
go test ./...                       # 运行所有测试
go test -race ./internal/sitesync ./internal/op # 并发敏感改动的定向测试
gofmt -w path/to/changed.go         # 修改 Go 文件后格式化
```

### 前端 (Next.js)
```bash
cd web
pnpm install                        # 安装依赖
pnpm dev                            # 开发服务器 (localhost:3000)
NEXT_PUBLIC_API_BASE_URL="http://127.0.0.1:8080" pnpm dev  # 指定后端地址
pnpm build                          # 生产构建 (输出到 web/out/)
pnpm lint                           # ESLint 检查
pnpm exec tsc --noEmit --incremental false # TypeScript 检查
```

### 完整构建
```bash
cd web && pnpm install && pnpm build && cd ..
rm -rf static/out && mv web/out static/
go build -o build/octopus .
./build/octopus start
```

`static/static.go` 使用 `//go:embed all:out` 嵌入 `static/out`。只修改 `web/src` 或只生成 `web/out` 不会更新 8080 服务，必须同步 `static/out` 后重新编译 Go。`static/out/`、`web/out/` 和 `build/` 是被忽略的生成目录；执行删除命令前必须确认目标没有扩大到 `data/` 或其他用户文件。

### 跨平台发布
```bash
./scripts/build.sh build linux x86_64   # 构建指定平台
./scripts/build.sh release              # 构建所有平台
```

### Docker
```bash
docker compose up -d
```

## 架构概览

Octopus 是一个 **LLM API 聚合与负载均衡服务**。Go 后端 (Gin + GORM) 提供 API 代理和管理接口，Next.js 前端提供管理面板。

**启动流程**: `main.go` → `cmd/start.go` → 初始化 Config → DB → Cache → HTTP Server → Background Tasks

**请求流**: Gin Router → Middleware (Auth/CORS/Logger) → Handler → Op (业务逻辑) → DB/Cache

**API 代理流**: Request → Inbound Transformer (协议转换) → Relay → Balancer (负载均衡/熔断) → 外部 LLM API → Outbound Transformer → Response

## 后端关键模块 (`internal/`)

| 模块 | 职责 |
|------|------|
| `conf/` | Viper 配置管理，env 前缀 `OCTOPUS_`，默认读取 `data/config.json` |
| `db/` | GORM 数据库层，支持 SQLite(默认)/MySQL/PostgreSQL，`db/migrate/` 含迁移 |
| `model/` | 数据模型定义 (Channel, Group, User, APIKey, Setting, Stats 等) |
| `op/` | **业务逻辑层 (Service)**，包含内存缓存管理，Handler 调用此层而非直接操作 DB |
| `server/handlers/` | HTTP 请求处理器，按资源分文件 |
| `server/middleware/` | Auth (JWT + API Key)、CORS、Logger、Static 等中间件 |
| `server/router/` | 自定义路由框架，链式注册: `NewGroupRouter(path).Use(mw).AddRoute(route)` |
| `server/auth/` | JWT 生成/验证，API Key 格式 `sk-octopus-*` |
| `server/resp/` | 统一响应格式 `{code, message, data}` |
| `relay/` | API 代理核心，负载均衡策略 (RoundRobin/Random/Failover/Weighted)，熔断器 |
| `transformer/` | 协议转换适配器，`inbound/` 解析请求，`outbound/` 格式化响应，支持 OpenAI/Anthropic/Gemini |
| `task/` | 后台定时任务 (统计持久化、模型同步、价格更新) |
| `client/` | LLM 提供商 HTTP 客户端封装 |
| `utils/log/` | Zap 结构化日志 |
| `utils/cache/` | 基于 xxhash 的泛型分片内存缓存；分片数由调用方决定，具体业务是否持久化由 `internal/op/` 调用方决定 |

## 前端关键模式 (`web/src/`)

- **状态管理**: Zustand (本地/持久化状态) + TanStack React Query (全局默认 stale time 当前为 60s，具体接口刷新周期不同)
- **UI**: shadcn/ui + Radix UI 原语 + TailwindCSS v4 + `motion/react` 动画
- **路由**: 自定义 SPA 路由 (`route/config.tsx` 定义，`ContentLoader` 动态加载)，**不使用** Next.js 文件路由
- **API 层**: `api/client.ts` 基于 fetch 的 HTTP 客户端，`api/endpoints/` 按功能导出 React Query hooks
- **i18n**: next-intl，翻译文件位于 `public/locale/{en,zh_hans,zh_hant}.json`
- **构建**: SSG 静态导出 (`output: "export"`)，嵌入到 Go 二进制的 `static/` 目录

## 配置

运行时配置 `data/config.json`（首次运行自动生成），常用字段可通过 `OCTOPUS_` 前缀环境变量覆盖:
- `OCTOPUS_SERVER_PORT`, `OCTOPUS_SERVER_HOST`
- `OCTOPUS_DATABASE_TYPE` (sqlite/mysql/postgres), `OCTOPUS_DATABASE_PATH`
- `OCTOPUS_LOG_LEVEL`

数据库运行时设置 (CORS 等) 存储在 `Setting` 模型中，通过 `op/setting.go` 缓存访问。

## 网络、认证与 CORS

- 默认后端监听 `0.0.0.0:8080`，前端开发服务器通常监听 `3000`。
- 生产内嵌页面优先从后端同源访问，例如 `http://<host>:8080`。前后端分离开发时，`NEXT_PUBLIC_API_BASE_URL="http://127.0.0.1:8080"` 只适用于浏览器和后端在同一台机器的场景；局域网客户端必须使用可达的后端主机名或 IP。
- `web/src/api/client.ts` 默认使用相对 API 地址 `.`。Next 开发服务器没有后端 API rewrite；跨源访问需要真实可达的 API 地址和 CORS 配置。
- `cors_allow_origins` 默认为空，即拒绝跨源浏览器请求；跨源开发必须在数据库 `Setting` 中配置准确的来源（协议、主机和端口）。不要为了绕过地址问题直接放宽为 `*`。
- 管理 API 使用 JWT `Authorization: Bearer ...`；`/v1` 代理接口使用 `sk-octopus-*` API Key。认证、站点 Access Token、代理凭据和完整请求头不得写入日志、测试 fixture、截图、文档或提交。
- 浏览器出现 `Failed to fetch` 时，先检查进程、端口、浏览器实际请求 URL、协议和 CORS；到达后端的业务错误通常会返回 JSON 4xx/5xx，而不是网络层异常。

## 数据库与迁移安全

- 默认 SQLite 数据库是 `data/data.db`；`data/` 被忽略，但包含真实运行数据，不能清空、覆盖、复制到提交或测试 fixture。
- 启动会执行 `BeforeAutoMigrate`、GORM `AutoMigrate` 和 `AfterAutoMigrate`；数据库 schema 变化可能发生在服务启动阶段。
- 修改模型字段类型、nullable、索引或关联关系时，必须检查 SQLite、MySQL、PostgreSQL 的迁移行为，并优先增加隔离的内存/临时数据库测试。不要把 SQLite 通过的迁移当成其他数据库已验证。
- 未经用户明确确认，不对现有数据库执行迁移、批量清理、导入、导出或恢复；不要使用 `git reset --hard`、`git checkout --` 或宽泛 `rm -rf`。
- SQLite 使用单写连接策略；测试和运行中的并发写入要考虑 `SQLITE_BUSY` 和迁移锁，不要让测试并行复用同一真实数据库文件。

## 测试与交付检查

- Go 修改：运行 `gofmt` 和受影响包的 `go test`；跨层、生命周期或并发改动再运行定向 `go test -race`。
- 前端修改：运行 `pnpm lint`、`pnpm exec tsc --noEmit --incremental false`；构建相关改动运行 `pnpm build`。
- 嵌入前端改动：确认 `static/out` 已更新并重新编译 Go，不要只验证 `web/out`。
- 完成前查看 `git diff --check`、`git diff --stat` 和 `git status`，确认没有覆盖用户已有改动或混入秘密/生成文件。
- 无法运行的测试、未验证的数据库方言、后台任务或真实上游行为必须在交付时明确说明。

## 版本与更新日志

- 版本标签使用 annotated tag，格式必须为 `vMAJOR.MINOR.PATCH`；GitHub Actions 的发布工作流只接受 `v*.*.*`，并要求标签指向 `main` 上的提交。
- 本仓库历史上没有持久化更新日志文件；正式版本说明写在 annotated tag 的 message 中，由 `.github/workflows/release.yaml` 的 `changelogithub` 生成 GitHub Release。不要为单次发布擅自新增 `CHANGELOG.md`。
- Tag message 使用版本标题、简短摘要和分类条目（新增 / 修复 / 兼容性 / 文档 / 验证）；条目写用户可感知的结果，不泄露凭据、真实站点 ID 或本地路径。
- 发布前先检查 `git log <previous-tag>..HEAD`、当前 diff、远程同名 tag 和 `main` 分支状态。正式流程为：通过测试和构建 → 准备 tag message → 查看完整 diff → 提交 → 创建 annotated tag → 推送 `main` 和对应 tag → 检查 GitHub Actions / Release 结果。未经用户明确要求，不执行 tag、推送、发布或 Docker 镜像上传。
- 发布前确认构建产物只来自源码和 `static/out`，`data/`、凭据、本地日志、数据库文件、`build/` 临时产物和 `web/out/` 不得进入提交。

## 变更范围与协作

- 先读相关代码、测试和本文件，做最小必要改动；沿用现有模块和错误处理模式。
- 不要回滚、覆盖或格式化与当前任务无关的用户改动；不要创建/切换分支、提交、打 tag、推送、发布或创建 PR，除非用户明确要求。
- 不要为单一需求引入新的通用抽象；注释只说明不明显的意图。
- 修改公共 JSON/API 契约时，搜索所有生产调用方、导入/导出、迁移和测试，并保留向后兼容或明确迁移策略。
- 运行长期服务前确认端口未被占用；停止或重启已有服务前先确认进程归属，不要误杀用户进程。
- AI 辅助代码必须经过人工/独立 review；最终交付说明修改内容、验证命令、已知风险和未验证范围。

## 对旧规则的核对

原 `CLAUDE.md` 的架构分层、默认配置路径、`OCTOPUS_` 前缀、数据库类型、管理 API/代理 API 的职责划分、SPA 路由和静态嵌入方向均正确，已保留。

已按当前仓库事实修正或细化：

- Go module 是 `1.25.0`；旧 README 中的 `1.24.4` 不能作为当前版本依据。
- 动画库实际是 `motion`，代码从 `motion/react` 导入，不是独立的 `framer-motion` 依赖。
- React Query 不是统一 30 秒刷新：全局 stale time 当前为 60 秒，各 endpoint 的刷新间隔不同。
- 生产前端必须经过 `web/out -> static/out -> Go 编译` 才能更新 8080。
- `127.0.0.1:8080` 只适用于同机开发；局域网设备访问开发前端时必须使用可达的后端地址，或直接访问 8080 的同源生产页面。
- CORS 默认关闭跨源来源；跨源开发需要配置数据库 Setting，而不是只修改前端环境变量。
- `utils/cache` 本身只是分片内存缓存；是否写回数据库是具体业务缓存的行为，不能概括为所有缓存都关机持久化。

## 贡献规范

- 每个 PR 只包含一个变更主题（一个功能或一个 BUG 修复）
- AI 辅助代码需完成人工审查后提交
