# db-hammer MCP Server 功能实施计划

## 📋 项目概述

### 项目目标
为db-hammer项目添加MCP (Model Context Protocol) server功能，让AI助手能够通过MCP协议直接操作各种数据库，提供安全、高效的数据库访问能力。

### 技术栈
- **核心框架**: fastmcp
- **数据库支持**: MySQL, PostgreSQL, Oracle, MSSQL, SQLite
- **Python版本**: 3.7+
- **认证方式**: 支持多种认证提供商

## 🎯 功能范围

### 核心功能
1. **数据库连接管理** - 支持多种数据库的连接创建和管理
2. **SQL查询执行** - 安全的SQL查询执行和结果返回
3. **数据操作** - 增删改查操作
4. **数据导出** - 大数据集的高性能导出
5. **结构查询** - 数据库表结构和元数据查询
6. **资源管理** - 基于MCP协议的资源访问

### 高级功能
1. **连接池管理** - 优化数据库连接使用
2. **分页查询** - 大数据集的分页处理
3. **流式处理** - 大表数据的流式导出
4. **安全认证** - 多层安全保护机制
5. **操作审计** - 完整的操作日志记录

## 📁 项目结构

```
db_hammer/
├── mcp/                          # MCP功能模块
│   ├── __init__.py              # MCP模块初始化
│   ├── server.py                # MCP server主文件
│   ├── config.py                # MCP配置管理
│   ├── auth.py                  # 认证和授权
│   ├── exceptions.py            # MCP相关异常
│   ├── web/                     # Web配置界面
│   │   ├── __init__.py
│   │   ├── app.py               # Flask Web应用
│   │   ├── api.py               # Web API接口
│   │   ├── config_manager.py    # 配置管理器
│   │   └── connection_tester.py # 连接测试器
│   ├── tools/                   # MCP工具实现
│   │   ├── __init__.py
│   │   ├── connection.py        # 数据库连接工具
│   │   ├── query.py             # 查询执行工具
│   │   ├── crud.py              # CRUD操作工具
│   │   ├── export.py            # 数据导出工具
│   │   ├── admin.py             # 数据库管理工具
│   │   └── schema.py            # 结构查询工具
│   ├── resources/               # MCP资源定义
│   │   ├── __init__.py
│   │   ├── tables.py            # 表结构资源
│   │   ├── schemas.py           # 数据库模式资源
│   │   └── connections.py       # 连接状态资源
│   ├── utils/                   # MCP工具函数
│   │   ├── __init__.py
│   │   ├── security.py          # 安全工具
│   │   ├── formatter.py         # 数据格式化
│   │   └── validator.py         # 参数验证
│   └── templates/               # 配置模板
│       ├── config.yaml          # 基础配置模板
│       └── connections.yaml     # 连接配置模板
├── web/                         # Web界面文件
│   ├── templates/               # HTML模板
│   │   ├── config.html          # 配置页面模板
│   │   └── base.html            # 基础模板
│   ├── static/                  # 静态文件
│   │   ├── css/
│   │   │   └── config.css       # 配置页面样式
│   │   ├── js/
│   │   │   ├── ace.js           # Ace编辑器
│   │   │   ├── mode-json.js     # JSON模式
│   │   │   └── config.js        # 配置页面脚本
│   │   └── img/                 # 图片资源
│   └── downloads/               # 第三方库下载脚本
│       └── download_ace.js      # 下载Ace.js脚本
├── cli/                         # 命令行工具
│   ├── __init__.py
│   └── mcp_server.py            # MCP server启动脚本
├── tests/                       # 测试文件
│   ├── test_mcp_server.py       # MCP server测试
│   ├── test_mcp_tools.py        # MCP工具测试
│   ├── test_web_config.py       # Web配置界面测试
│   └── fixtures/                # 测试数据
│       ├── test_connections.yaml
│       └── test_config.json
├── docs/                        # 文档
│   ├── MCP_USAGE.md             # MCP使用指南
│   ├── MCP_API.md               # MCP API文档
│   ├── WEB_CONFIG.md            # Web配置界面说明
│   └── MCP_DEPLOYMENT.md        # 部署指南
├── requirements.txt             # 更新依赖
├── setup.py                     # 更新包配置
└── README-MCP.md                # MCP功能说明
```

## 🚀 实施阶段

### 第一阶段：基础框架搭建 (1-2天)

#### 1.1 项目结构调整
- [ ] 创建 `db_hammer/mcp/` 目录结构
- [ ] 更新 `db_hammer/__init__.py` 导出MCP模块
- [ ] 更新 `requirements.txt` 添加fastmcp依赖
- [ ] 更新 `setup.py` 添加MCP相关配置

#### 1.2 基础配置系统
- [ ] 实现 `mcp/config.py` 配置管理类
- [ ] 创建配置模板文件
- [ ] 实现环境变量支持
- [ ] 添加配置验证功能

#### 1.3 异常处理机制
- [ ] 创建 `mcp/exceptions.py` MCP专用异常
- [ ] 实现统一的错误处理机制
- [ ] 添加详细的错误日志记录

### 第二阶段：核心MCP工具开发 (3-4天)

#### 2.1 数据库连接管理
```python
# 需要实现的主要工具
@mcp.tool
def create_mysql_connection(host: str, user: str, password: str, database: str, port: int = 3306) -> str
def create_postgresql_connection(host: str, user: str, password: str, database: str, port: int = 5432) -> str
def create_oracle_connection(host: str, user: str, password: str, service_name: str, port: int = 1521) -> str
def create_mssql_connection(host: str, user: str, password: str, database: str, port: int = 1433) -> str
def create_sqlite_connection(database_path: str) -> str
def close_connection(connection_id: str) -> bool
def list_connections() -> list
```

#### 2.2 SQL查询执行
```python
# 查询相关工具
@mcp.tool
def execute_query(connection_id: str, sql: str, params: dict = None) -> list
def execute_select(connection_id: str, table: str, columns: list = None, where: str = None, limit: int = None) -> list
def execute_query_with_pagination(connection_id: str, sql: str, page_size: int = 100, page: int = 1) -> dict
def explain_query(connection_id: str, sql: str) -> dict
```

#### 2.3 CRUD操作工具
```python
# 数据操作工具
@mcp.tool
def insert_data(connection_id: str, table: str, data: dict) -> int
def update_data(connection_id: str, table: str, data: dict, where: str) -> int
def delete_data(connection_id: str, table: str, where: str) -> int
def upsert_data(connection_id: str, table: str, data: dict, conflict_columns: list) -> int
```

#### 2.4 数据导出功能
```python
# 导出相关工具
@mcp.tool
def export_table_data(connection_id: str, table: str, format: str = "csv", where: str = None) -> dict
def export_query_data(connection_id: str, sql: str, format: str = "csv") -> dict
def stream_large_table(connection_id: str, table: str, batch_size: int = 1000, format: str = "csv") -> dict
def get_export_status(export_id: str) -> dict
def download_export_file(export_id: str) -> str
def list_export_files() -> list
def delete_export_file(export_id: str) -> bool
```

**数据导出架构设计**:
```python
class ExportManager:
    def __init__(self, storage_path="./exports"):
        self.storage_path = storage_path
        self.active_exports = {}
        self.export_queue = []

    def create_export_task(self, connection_id: str, export_type: str, params: dict) -> str:
        """创建导出任务，返回export_id"""
        export_id = str(uuid.uuid4())

        task = {
            "export_id": export_id,
            "connection_id": connection_id,
            "export_type": export_type,  # table, query, stream
            "params": params,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "file_path": None,
            "file_size": 0,
            "download_url": None,
            "error": None
        }

        self.active_exports[export_id] = task
        return export_id

    def execute_export(self, export_id: str) -> dict:
        """执行导出任务"""
        task = self.active_exports.get(export_id)
        if not task:
            raise Exception(f"导出任务不存在: {export_id}")

        try:
            task["status"] = "running"

            # 生成文件路径
            file_name = f"{export_id}.{task['params'].get('format', 'csv')}"
            file_path = os.path.join(self.storage_path, file_name)

            # 执行导出
            if task["export_type"] == "table":
                self._export_table(task, file_path)
            elif task["export_type"] == "query":
                self._export_query(task, file_path)
            elif task["export_type"] == "stream":
                self._export_stream(task, file_path)

            # 更新任务状态
            task["status"] = "completed"
            task["file_path"] = file_path
            task["file_size"] = os.path.getsize(file_path)
            task["download_url"] = f"/api/exports/download/{export_id}"
            task["completed_at"] = datetime.now().isoformat()

        except Exception as e:
            task["status"] = "failed"
            task["error"] = str(e)

        return task

    def get_download_url(self, export_id: str) -> str:
        """获取下载URL"""
        task = self.active_exports.get(export_id)
        if not task or task["status"] != "completed":
            raise Exception(f"导出文件不可用: {export_id}")

        return task["download_url"]
```

**导出API接口设计**:
```python
# Flask API 路由
@app.route('/api/exports', methods=['POST'])
def create_export():
    """创建导出任务"""
    data = request.json
    connection_id = data.get('connection_id')
    export_type = data.get('export_type')  # table, query, stream
    params = data.get('params', {})

    export_manager = ExportManager()
    export_id = export_manager.create_export_task(connection_id, export_type, params)

    # 异步执行导出任务
    threading.Thread(
        target=export_manager.execute_export,
        args=(export_id,),
        daemon=True
    ).start()

    return jsonify({
        "export_id": export_id,
        "status": "pending",
        "message": "导出任务已创建"
    })

@app.route('/api/exports/<export_id>/status', methods=['GET'])
def get_export_status(export_id):
    """获取导出状态"""
    export_manager = ExportManager()
    task = export_manager.active_exports.get(export_id)

    if not task:
        return jsonify({"error": "导出任务不存在"}), 404

    return jsonify({
        "export_id": export_id,
        "status": task["status"],
        "file_size": task.get("file_size", 0),
        "download_url": task.get("download_url"),
        "error": task.get("error"),
        "created_at": task["created_at"],
        "completed_at": task.get("completed_at")
    })

@app.route('/api/exports/<export_id>/download', methods=['GET'])
def download_export_file(export_id):
    """下载导出文件"""
    export_manager = ExportManager()
    task = export_manager.active_exports.get(export_id)

    if not task or task["status"] != "completed":
        return jsonify({"error": "导出文件不可用"}), 404

    file_path = task["file_path"]
    if not os.path.exists(file_path):
        return jsonify({"error": "文件不存在"}), 404

    # 获取文件名
    original_filename = f"export_{export_id}.{task['params'].get('format', 'csv')}"

    return send_file(
        file_path,
        as_attachment=True,
        download_name=original_filename,
        mimetype=_get_mime_type(task['params'].get('format', 'csv'))
    )

@app.route('/api/exports', methods=['GET'])
def list_exports():
    """列出所有导出任务"""
    export_manager = ExportManager()
    exports = []

    for export_id, task in export_manager.active_exports.items():
        exports.append({
            "export_id": export_id,
            "connection_id": task["connection_id"],
            "export_type": task["export_type"],
            "status": task["status"],
            "file_size": task.get("file_size", 0),
            "download_url": task.get("download_url"),
            "created_at": task["created_at"],
            "completed_at": task.get("completed_at")
        })

    return jsonify({"exports": exports})

@app.route('/api/exports/<export_id>', methods=['DELETE'])
def delete_export(export_id):
    """删除导出任务和文件"""
    export_manager = ExportManager()
    task = export_manager.active_exports.get(export_id)

    if not task:
        return jsonify({"error": "导出任务不存在"}), 404

    # 删除文件
    if task.get("file_path") and os.path.exists(task["file_path"]):
        os.remove(task["file_path"])

    # 删除任务
    del export_manager.active_exports[export_id]

    return jsonify({"message": "导出任务已删除"})

def _get_mime_type(format: str) -> str:
    """获取文件MIME类型"""
    mime_types = {
        "csv": "text/csv",
        "json": "application/json",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "txt": "text/plain",
        "gz": "application/gzip"
    }
    return mime_types.get(format, "application/octet-stream")
```

### 第三阶段：高级功能开发 (2-3天)

#### 3.1 MCP资源系统
```python
# 资源定义
@mcp.resource("databases://{connection_id}/tables")
def list_tables(connection_id: str) -> list

@mcp.resource("databases://{connection_id}/tables/{table_name}/schema")
def get_table_schema(connection_id: str, table_name: str) -> dict

@mcp.resource("databases://{connection_id}/tables/{table_name}/data")
def get_table_data(connection_id: str, table_name: str, limit: int = 100) -> list

@mcp.resource("databases://{connection_id}/status")
def get_connection_status(connection_id: str) -> dict
```

#### 3.2 安全认证系统
- [ ] 实现基于API密钥的认证
- [ ] 支持JWT token认证
- [ ] 集成企业认证提供商（Google, GitHub等）
- [ ] 实现权限控制和角色管理

#### 3.3 操作审计和日志
- [ ] 实现操作日志记录
- [ ] 添加SQL执行审计
- [ ] 实现敏感操作告警
- [ ] 创建审计报告生成工具

#### 3.4 性能优化
- [ ] 实现连接池管理
- [ ] 添加查询结果缓存
- [ ] 优化大数据集处理
- [ ] 实现异步操作支持

### 第四阶段：Web配置界面和部署 (2-3天)

#### 4.1 Web配置管理界面
- [ ] 创建Flask Web应用框架
- [ ] 设计简洁的配置管理页面
- [ ] 集成Ace.js JSON编辑器
- [ ] 实现多连接配置管理

**技术实现要点**:
- 使用Flask模板引擎，避免前端工程
- Ace.js编辑器从CDN下载到本地静态文件
- 全屏JSON编辑区域，简洁UI设计
- 保存和格式化按钮功能

**页面功能**:
```python
# 路由和功能实现
@app.route('/')
def config_page():
    return render_template('config.html')

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(current_config)

@app.route('/api/config', methods=['POST'])
def save_config():
    config_data = request.json
    # 验证配置格式
    # 多线程测试连接
    # 返回测试结果
    return jsonify(result)

@app.route('/api/test-connections', methods=['POST'])
def test_connections():
    connections = request.json.get('connections', [])
    # 多线程并发测试，5秒超时
    results = test_connections_async(connections)
    return jsonify(results)
```

**前端实现**:
```html
<!-- templates/config.html 结构 -->
<!DOCTYPE html>
<html>
<head>
    <title>db-hammer MCP 配置</title>
    <script src="/static/js/ace.js"></script>
</head>
<body>
    <div class="header">
        <h1>db-hammer MCP Server 配置</h1>
        <div class="actions">
            <button onclick="formatJSON()">格式化</button>
            <button onclick="saveConfig()">保存配置</button>
        </div>
    </div>
    <div class="editor-container">
        <div id="json-editor" style="width: 100%; height: calc(100vh - 80px);"></div>
    </div>
    <div id="test-results" class="test-results"></div>
</body>
</html>
```

**配置格式示例**:
```json
{
  "server": {
    "host": "localhost",
    "port": 8080,
    "debug": false
  },
  "connections": [
    {
      "id": "mysql_main",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "user": "root",
      "password": "password",
      "database": "test_db",
      "name": "主MySQL数据库"
    },
    {
      "id": "postgres_analytics",
      "type": "postgresql",
      "host": "localhost",
      "port": 5432,
      "user": "postgres",
      "password": "password",
      "database": "analytics",
      "name": "分析PostgreSQL数据库"
    }
  ]
}
```

**多线程连接测试实现**:
```python
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def test_single_connection(connection_config):
    """测试单个数据库连接"""
    start_time = time.time()
    try:
        # 根据连接类型创建连接
        conn = create_connection_from_config(connection_config)
        # 执行简单查询测试连接
        result = conn.select_value("SELECT 1")
        duration = time.time() - start_time

        return {
            "connection_id": connection_config["id"],
            "status": "success",
            "duration": round(duration, 2),
            "message": "连接成功"
        }
    except Exception as e:
        duration = time.time() - start_time
        return {
            "connection_id": connection_config["id"],
            "status": "failed",
            "duration": round(duration, 2),
            "message": f"连接失败: {str(e)}"
        }

def test_connections_async(connections, timeout=5):
    """多线程并发测试连接"""
    with ThreadPoolExecutor(max_workers=len(connections)) as executor:
        future_to_conn = {
            executor.submit(test_single_connection, conn): conn
            for conn in connections
        }

        results = []
        for future in as_completed(future_to_conn, timeout=timeout):
            try:
                result = future.result(timeout=timeout)
                results.append(result)
            except Exception as e:
                conn = future_to_conn[future]
                results.append({
                    "connection_id": conn["id"],
                    "status": "timeout",
                    "duration": timeout,
                    "message": "连接超时"
                })

    return results
```

#### 4.2 命令行工具
- [ ] 创建 `db_hammer_mcp` 命令行工具
- [ ] 实现配置文件生成向导
- [ ] 添加服务状态检查功能
- [ ] 实现日志查看和管理
- [ ] 集成Web界面启动选项

#### 4.3 配置管理系统
- [ ] 创建配置文件模板
- [ ] 实现配置热重载
- [ ] 添加配置验证功能
- [ ] 支持多环境配置
- [ ] Web界面配置同步

#### 4.4 部署和文档
- [ ] 编写部署指南
- [ ] 创建Docker镜像
- [ ] 编写API文档
- [ ] 创建使用示例和教程
- [ ] 添加Web配置界面使用说明

## 🌐 Web配置界面详细设计

### 技术架构
- **后端**: Flask轻量级Web框架
- **前端**: 原生HTML + CSS + JavaScript
- **编辑器**: Ace.js (本地部署，无需CDN依赖)
- **通信**: RESTful API + JSON格式
- **文件服务**: Flask文件下载服务
- **部署**: 内嵌Flask服务器，支持独立运行

### 界面设计原则
- **极简主义**: 只有必要的功能元素
- **功能聚焦**: 以JSON配置编辑为核心
- **响应式**: 支持不同屏幕尺寸
- **用户友好**: 清晰的错误提示和操作反馈

### 核心功能模块

#### 1. 配置编辑器
```javascript
// Ace.js编辑器配置
const editor = ace.edit("json-editor");
editor.setTheme("ace/theme/monokai");
editor.session.setMode("ace/mode/json");
editor.setOptions({
    fontSize: "14px",
    showPrintMargin: false,
    enableBasicAutocompletion: true,
    enableLiveAutocompletion: true
});
```

#### 2. 配置验证
```python
def validate_config(config_data):
    """验证配置格式和必需字段"""
    required_fields = ['server', 'connections']
    errors = []

    # 验证顶级字段
    for field in required_fields:
        if field not in config_data:
            errors.append(f"缺少必需字段: {field}")

    # 验证连接配置
    if 'connections' in config_data:
        for i, conn in enumerate(config_data['connections']):
            if 'id' not in conn:
                errors.append(f"连接[{i}]缺少id字段")
            if 'type' not in conn:
                errors.append(f"连接[{i}]缺少type字段")
            if conn.get('type') not in ['mysql', 'postgresql', 'oracle', 'mssql', 'sqlite']:
                errors.append(f"连接[{i}]类型不支持: {conn.get('type')}")

    return errors
```

#### 3. 连接池管理
```python
class ConnectionPool:
    def __init__(self):
        self.connections = {}
        self.last_used = {}

    def get_connection(self, connection_id, config):
        if connection_id not in self.connections:
            self.connections[connection_id] = create_connection_from_config(config)
        self.last_used[connection_id] = time.time()
        return self.connections[connection_id]

    def cleanup_idle_connections(self, max_idle_time=300):
        current_time = time.time()
        to_remove = []
        for conn_id, last_used in self.last_used.items():
            if current_time - last_used > max_idle_time:
                to_remove.append(conn_id)

        for conn_id in to_remove:
            if conn_id in self.connections:
                self.connections[conn_id].close()
                del self.connections[conn_id]
                del self.last_used[conn_id]
```

### API接口设计

#### GET /api/config
获取当前配置
```json
{
    "server": {
        "host": "localhost",
        "port": 8080,
        "debug": false
    },
    "connections": [...]
}
```

#### POST /api/config
保存配置并测试连接
```json
{
    "config": {...},
    "test_connections": true
}

// 响应
{
    "success": true,
    "message": "配置保存成功",
    "test_results": [
        {
            "connection_id": "mysql_main",
            "status": "success",
            "duration": 0.15,
            "message": "连接成功"
        }
    ]
}
```

#### POST /api/test-connections
测试所有连接
```json
{
    "connections": [...]
}

// 响应
{
    "results": [
        {
            "connection_id": "mysql_main",
            "status": "success",
            "duration": 0.15,
            "message": "连接成功"
        }
    ]
}
```

### 第三方库集成

#### Ace.js本地部署脚本
```python
# web/downloads/download_ace.js
import requests
import os
import zipfile

def download_ace_editor():
    """下载Ace.js到本地static目录"""
    ace_version = "1.32.2"
    download_url = f"https://github.com/ajaxorg/ace-builds/archive/refs/tags/v{ace_version}.zip"

    static_js_dir = "web/static/js"
    os.makedirs(static_js_dir, exist_ok=True)

    # 下载zip文件
    response = requests.get(download_url)
    zip_path = "/tmp/ace.zip"
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    # 解压并复制必要文件
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # 复制src-min到static/js
        for file in zip_ref.namelist():
            if file.startswith(f"ace-builds-v{ace_version}/src-min-noconflict/"):
                filename = os.path.basename(file)
                if filename:
                    with zip_ref.open(file) as source, open(os.path.join(static_js_dir, filename), "wb") as target:
                        target.write(source.read())

    os.remove(zip_path)
    print(f"Ace.js v{ace_version}下载完成")

if __name__ == "__main__":
    download_ace_editor()
```

### CSS样式设计
```css
/* web/static/css/config.css */
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    background-color: #1e1e1e;
    color: #d4d4d4;
    height: 100vh;
    display: flex;
    flex-direction: column;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 15px 20px;
    background-color: #2d2d30;
    border-bottom: 1px solid #3e3e42;
    height: 60px;
}

.header h1 {
    font-size: 18px;
    font-weight: 500;
    color: #ffffff;
}

.actions {
    display: flex;
    gap: 10px;
}

button {
    padding: 8px 16px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: background-color 0.2s;
}

.btn-primary {
    background-color: #007acc;
    color: white;
}

.btn-primary:hover {
    background-color: #005a9e;
}

.btn-secondary {
    background-color: #3c3c3c;
    color: white;
}

.btn-secondary:hover {
    background-color: #4a4a4a;
}

.editor-container {
    flex: 1;
    position: relative;
}

.test-results {
    position: fixed;
    bottom: 20px;
    right: 20px;
    background-color: #2d2d30;
    border: 1px solid #3e3e42;
    border-radius: 4px;
    padding: 15px;
    max-width: 400px;
    max-height: 200px;
    overflow-y: auto;
    z-index: 1000;
}

.test-result-item {
    display: flex;
    align-items: center;
    margin-bottom: 8px;
    font-size: 14px;
}

.test-result-item.success {
    color: #4ec9b0;
}

.test-result-item.failed {
    color: #f44747;
}

.test-result-item.timeout {
    color: #ce9178;
}
```

### JavaScript功能实现
```javascript
// web/static/js/config.js
let editor;
let currentConfig = {};

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    initializeEditor();
    loadCurrentConfig();
});

function initializeEditor() {
    editor = ace.edit("json-editor");
    editor.setTheme("ace/theme/monokai");
    editor.session.setMode("ace/mode/json");
    editor.setOptions({
        fontSize: "14px",
        showPrintMargin: false,
        enableBasicAutocompletion: true,
        enableLiveAutocompletion: true,
        wrap: true
    });

    // 监听内容变化
    editor.session.on('change', function() {
        clearTestResults();
    });
}

async function loadCurrentConfig() {
    try {
        const response = await fetch('/api/config');
        const config = await response.json();
        currentConfig = config;
        editor.setValue(JSON.stringify(config, null, 2), -1);
    } catch (error) {
        console.error('加载配置失败:', error);
    }
}

function formatJSON() {
    try {
        const config = JSON.parse(editor.getValue());
        editor.setValue(JSON.stringify(config, null, 2), -1);
    } catch (error) {
        showMessage('JSON格式错误: ' + error.message, 'error');
    }
}

async function saveConfig() {
    try {
        const config = JSON.parse(editor.getValue());

        // 显示测试结果
        showTestResults([{connection_id: 'all', status: 'testing', message: '正在测试连接...'}]);

        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                config: config,
                test_connections: true
            })
        });

        const result = await response.json();

        if (result.success) {
            showMessage('配置保存成功', 'success');
            showTestResults(result.test_results);
        } else {
            showMessage('配置保存失败: ' + result.message, 'error');
        }
    } catch (error) {
        showMessage('保存配置失败: ' + error.message, 'error');
    }
}

function showTestResults(results) {
    const container = document.getElementById('test-results');
    container.innerHTML = '';

    results.forEach(result => {
        const item = document.createElement('div');
        item.className = `test-result-item ${result.status}`;

        let icon = '';
        if (result.status === 'success') icon = '✓';
        else if (result.status === 'failed') icon = '✗';
        else if (result.status === 'testing') icon = '⏳';
        else if (result.status === 'timeout') icon = '⏱';

        item.innerHTML = `
            <span>${icon}</span>
            <span style="margin-left: 8px;">${result.connection_id}: ${result.message}</span>
            <span style="margin-left: auto; font-size: 12px;">${result.duration}s</span>
        `;

        container.appendChild(item);
    });

    container.style.display = 'block';
}

function clearTestResults() {
    const container = document.getElementById('test-results');
    container.style.display = 'none';
}

function showMessage(message, type) {
    // 简单的消息提示实现
    const messageDiv = document.createElement('div');
    messageDiv.style.cssText = `
        position: fixed;
        top: 20px;
        left: 50%;
        transform: translateX(-50%);
        background-color: ${type === 'success' ? '#4ec9b0' : '#f44747'};
        color: white;
        padding: 10px 20px;
        border-radius: 4px;
        z-index: 1001;
    `;
    messageDiv.textContent = message;

    document.body.appendChild(messageDiv);

    setTimeout(() => {
        document.body.removeChild(messageDiv);
    }, 3000);
}
```

## 🔧 技术实现要点

### 连接管理策略
1. **连接池**: 使用连接池避免频繁创建/销毁连接
2. **连接复用**: 智能管理连接生命周期
3. **异常恢复**: 自动重连和故障转移机制
4. **资源清理**: 确保连接和内存的正确释放

### 安全性保障
1. **SQL注入防护**: 使用参数化查询和输入验证
2. **权限控制**: 基于角色的访问控制（RBAC）
3. **数据加密**: 敏感信息的加密存储和传输
4. **审计追踪**: 完整的操作日志和审计记录

### 性能优化
1. **查询优化**: SQL语句分析和优化建议
2. **结果缓存**: 智能缓存查询结果
3. **流式处理**: 大数据集的流式读取和处理
4. **并发控制**: 合理的并发访问控制

### 错误处理
1. **统一异常**: 自定义MCP异常类型
2. **错误恢复**: 优雅的错误处理和恢复机制
3. **用户友好**: 清晰的错误信息和解决建议
4. **日志记录**: 详细的错误日志和调试信息

## 📦 依赖管理

### 新增依赖
```txt
# requirements.txt 新增内容
fastmcp>=0.1.0
pydantic>=2.0.0
PyYAML>=6.0.0
cryptography>=3.4.0
python-jose[cryptography]>=3.3.0
Flask>=2.3.0
requests>=2.31.0
```

### setup.py 更新
```python
# setup.py 中的 extras_require
extras_require={
    'mcp': [
        'fastmcp>=0.1.0',
        'pydantic>=2.0.0',
        'PyYAML>=6.0.0',
        'cryptography>=3.4.0',
        'python-jose[cryptography]>=3.3.0',
        'Flask>=2.3.0',
        'requests>=2.31.0',
    ]
}
```

## 🧪 测试策略

### 单元测试
- [ ] MCP工具函数测试
- [ ] 配置管理测试
- [ ] 异常处理测试
- [ ] 安全功能测试

### 集成测试
- [ ] 数据库连接测试
- [ ] CRUD操作测试
- [ ] 数据导出测试
- [ ] 权限控制测试

### 性能测试
- [ ] 大数据集查询测试
- [ ] 并发访问测试
- [ ] 内存使用测试
- [ ] 连接池性能测试

## 📖 文档规划

### 用户文档
1. **快速开始指南** - 5分钟上手教程
2. **配置指南** - 详细的配置说明
3. **API文档** - 完整的工具和资源参考
4. **最佳实践** - 安全和性能建议

### 开发者文档
1. **架构设计** - 系统架构说明
2. **扩展指南** - 自定义工具和资源开发
3. **贡献指南** - 开发和贡献流程
4. **故障排除** - 常见问题解决方案

## 🚀 部署方案

### 本地部署
```bash
pip install db-hammer[fastmcp]
db_hammer_mcp --config mcp_config.yaml
```

### Docker部署
```dockerfile
FROM python:3.9-slim
COPY . /app
WORKDIR /app
RUN pip install -e .[mcp]
CMD ["db_hammer_mcp", "--config", "/app/config/mcp_config.yaml"]
```

### 云服务部署
- [ ] AWS Lambda部署方案
- [ ] Google Cloud Functions部署方案
- [ ] Azure Functions部署方案
- [ ] Kubernetes集群部署方案

## 📊 成功指标

### 功能指标
- [ ] 支持所有主流数据库类型
- [ ] 实现100%的核心MCP工具
- [ ] 覆盖90%的常用数据库操作
- [ ] 提供5个以上的MCP资源

### 性能指标
- [ ] 查询响应时间 < 100ms (小数据集)
- [ ] 支持1000+并发连接
- [ ] 数据导出速度 > 10MB/s
- [ ] 内存使用 < 512MB (空闲状态)

### 安全指标
- [ ] 100%的SQL注入防护
- [ ] 支持企业级认证
- [ ] 完整的操作审计日志
- [ ] 敏感数据加密存储

## 🔄 后续规划

### 短期优化 (1-3个月)
- [ ] 添加更多数据库支持 (MongoDB, Redis等)
- [ ] 实现GraphQL查询支持
- [ ] 添加数据可视化功能
- [ ] 优化移动端支持

### 中期扩展 (3-6个月)
- [ ] 实现分布式部署
- [ ] 添加数据库迁移工具
- [ ] 集成机器学习模型
- [ ] 支持流式数据处理

### 长期愿景 (6-12个月)
- [ ] 构建数据库操作平台
- [ ] 实现智能SQL生成
- [ ] 添加自动化运维功能
- [ ] 支持多云部署方案

---

**文档版本**: v1.0
**创建日期**: 2025-01-07
**最后更新**: 2025-01-07
**负责人**: db-hammer开发团队