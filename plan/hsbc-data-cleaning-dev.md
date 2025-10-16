# 汇丰基金 PDF 数据清洗与增量向量化开发文档（草案）

本文档梳理季度基金 PDF 的清洗、增量输出与向量化衔接方案，确保与现有 n8n→Pinecone 工作流一致运作，同时控制向量库体量。

## 1. 背景与目标
- **输入来源**：季度爬虫下载的原始基金 PDF（含中英文页）；每日 TXT 的清洗在其他项目中完成，不在本项目范围。
- **核心目标**：
  1. 统一执行英文页剔除、章节解析、噪声过滤；
  2. 按章节构建文本指纹，实现增量更新与去重；
  3. 输出两类结果：
     - 文本切片（JSONL/CSV），可直接进入 n8n 工作流并写入 Pinecone；
     - 结构化数值（收益、持仓、费用等），供报表与变化分析；
  4. 维护 manifest/指纹，支持幂等与断点续处理。

## 2. 数据目录规划
- 默认输入目录 `raw/pdf/YYYY-Q/` 用于存放季度原始 PDF（命名 `{基金名称}_{基金代码}_{抓取时间戳}.pdf`），可通过 CLI 参数或配置文件将其指向本地磁盘、挂载的 NAS/NFS 路径，或同步自远程服务器的临时目录；
- `clean/pdf/YYYY-Q/`：英文页剔除后的 PDF 归档（可选）；
- `clean/chunks/YYYY-Q/`：文本切片输出（JSONL 或 CSV，每条含 `text` 与 `metadata`）；
- `outputs/structured/YYYY-Q/`：结构化表格/时间序列（例如十大持股、季度收益）；
- `state/processed_manifest.json`：记录文件处理状态、哈希、更新时间戳；
- `state/chunk_index.json`：缓存章节指纹与上一版 `chunk_hash`，用于增量对比。

## 3. 清洗流程
1. **文件调度**
   - 扫描 `raw/pdf` 下指定季度或基金的原始文件；
   - 读取 `state/processed_manifest.json`，若文件哈希/时间戳未变化则跳过；
   - 支持全量处理与增量模式。
2. **英文页剔除（预处理）**
   - 逐页统计中文字符数，剔除纯英文或空白页；
   - 输出剔除页码与处理版本，可选生成干净版 PDF 存放于 `clean/pdf`。
3. **内容解析**
   - 使用 `pdfplumber`/`pypdf` 抽取文本、页码与表格；
   - 依据标题关键字识别章节（“重要事项”“十大持股”“年度回报”等）；
   - 表格（十大持股、费用表等）解析为结构化数据。
4. **标准化与清洗**
   - 校验基金名称/代码与文件名一致；
   - 统一日期、币种、数值格式；
   - 清除页眉页脚、重复语句、无效空白；
   - 输出规范化章节文本。
5. **章节指纹与差异检测**
   - 计算指纹 `hash(fund_code + section + normalized_text)`；
   - 对比 `state/chunk_index.json` 上一次记录：
     * 指纹一致 → 标记 `reuse`，仅更新引用；
     * 指纹不同但相似度 > 0.98 → 可视业务决定是否跳过；
     * 差异显著 → 标记 `updated`/`new`，触发变化摘要生成。
6. **切片与输出**
   - 对需入库章节按 400~600 汉字切片（重叠 50~100）；
   - 每个 chunk 附带元数据：章节、页码、`chunk_hash`、`change_type` 等；
   - 结构化数据写入 `outputs/structured`，chunk metadata 中添加 `structured_refs`；
   - 对差异生成 `summary` 类型 chunk（持仓/收益变化说明）。
7. **落地与归档**
   - 文本 chunk 保存至 `clean/chunks/YYYY-Q/{fund_code}.jsonl`；
   - 更新 `processed_manifest`、`chunk_index`，记录处理时间、指纹、前一版本 hash；
   - 将清洗输出上传至 Google Drive “待处理”文件夹，触发 n8n。

## 4. 保留与忽略的内容
- **保留**：
  - 文档头部元信息（报告日期、基金名称/代码）；
  - 重要事项/风险摘要；
  - 投资组合概览（十大持股、行业分布等）；
  - 业绩表现（年度/累计/季度回报、基准比较）；
  - 产品资料概要（管理人、派息政策、最低投资额、费用等）；
  - 目标与策略描述；
  - 风险、费用、运营说明（NAV 计算、交易截止时间、联络渠道等）；
  - 变化摘要（季度间差异、关键字段变更）。
- **忽略/清理**：页脚页码、装饰性分隔符、空白行、重复标题、残留纯英文段落。

## 5. 文本 chunk 元数据规范
| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `fund_code` | str | 基金代码（与文件名一致） |
| `fund_name` | str | 基金中文名称 |
| `section` | str | 所属章节（如 `risk_summary`） |
| `page_range` | str | `start-end` 页码 |
| `chunk_index` | int | 同章节内的序号 |
| `chunk_hash` | str | 规范化文本指纹 |
| `previous_chunk_hash` | str/null | 若为 `updated`/`reuse`，指向上一版本指纹 |
| `change_type` | enum | `new` / `updated` / `reuse` / `summary` |
| `data_date` | date | 报告日期（无明确日期时使用抓取日期） |
| `file_timestamp` | datetime | 源文件生成或抓取时间 |
| `quarter` | str | 例如 `2025Q2` |
| `language` | enum | `zh` / `mix` |
| `source_type` | enum | 固定 `pdf` |
| `structured_refs` | array | 对应结构化记录 ID 列表 |
| `text` | str | chunk 正文 |
| `version` | str | 清洗程序版本号 |

## 6. 清洗后数据存放说明
- **存储位置**：`clean/chunks/YYYY-Q/{fund_code}.json`
- **文件命名**：`{基金名称或代码}_{季度}_{处理时间戳}.json`，例如 `聯博－美國增長基金_2025Q4_20251016T142016.json`
- **文件结构**：保存为 JSON 数组，每个元素包含：
  ```json
  {
    "type": "chunk" | "summary",
    "section": "...",
    "index": 0,
    "text": "...",
    "start_offset": 0,
    "end_offset": 600,
    "status": "new|updated|reuse"  // 仅 summary
  }
  ```
  这样上传后 MIME 为 `application/json`，n8n 可直接读取。
- **上传指引**：清洗完成后，将单个 JSON 文件上传至 Google Drive “待处理”文件夹（ID：`1PNFFxmkelrTRls98t3RH5AaQufL8V9GQ`）；保留本地副本用于归档。
- **归档策略**：每季度、每只基金一个文件，历史文件保留在 `clean/chunks/YYYY-Q`；可通过 `summary` 的 `status` 字段判断增量。

### 6.2 结构化数值
- **存储位置**：`outputs/structured/YYYY-Q/{fund_code}_{dataset}.csv` 或 `.parquet`
- **示例数据集**：
  - `fund_code_十大持股.csv`：字段 `fund_code, quarter, rank, company, sector, weight, source_page`
  - `fund_code_业绩表现.csv`：字段 `fund_code, quarter, metric, period, value, unit, source_page`
  - `fund_code_费用信息.csv`：字段 `fund_code, quarter, share_class, fee_type, value, unit`
- **用途**：供后续报表、变化摘要生成、与 TXT 净值数据合并使用；
- **归档策略**：同季度覆盖写入，并保留上一季度版本，便于差异比对。

### 6.3 状态与指纹记录
- `state/processed_manifest.json`：记录每份 PDF 的处理结果、生成时间、清洗版本；字段示例：
  ```json
  {
    "fund_code": "U62717",
    "quarter": "2025Q2",
    "file_path": "raw/pdf/2025-Q2/AB..._U62717_20251015.pdf",
    "file_hash": "...",
    "processed_at": "2025-10-16T01:02:03Z",
    "chunks_file": "clean/chunks/2025-Q2/U62717_2025Q2_20251016T010203.jsonl",
    "summary": {"new": 3, "updated": 2, "reuse": 5, "summary": 1}
  }
  ```
- `state/chunk_index.json`：维护章节指纹与上一版本 hash，结构示例：
  ```json
  {
    "U62717": {
      "2025Q1": {
        "risk_summary:0": {"hash": "hash1", "section": "risk_summary"},
        "top_holdings:5": {"hash": "hash2", "section": "top_holdings"}
      },
      "2025Q2": {
        "risk_summary:0": {"hash": "hash1", "section": "risk_summary"},   // 复用
        "top_holdings:5": {"hash": "hash3", "section": "top_holdings"}    // 更新
      }
    }
  }
  ```
- 这些状态文件支持幂等处理、增量对比、与 n8n 日志对齐。

### 6.4 持仓公司名录（新增）
- **目标**：在清洗每份基金 PDF 时提取“Top 10 Holdings”板块中的公司名称，维护一份去重后的公司列表，为后续构建持仓公司数据库做准备。
- **存储位置**：`outputs/structured/top_holdings_companies.csv`
  - 仅包含一列 `company_name`；清洗程序会统一去掉首尾空白、小写比较后追加，并保持集合有序写回。
- **处理流程**：章节解析后识别 `top_holdings` 段落，解析公司名称，剔除表头和统计行，调用写入模块刷新 CSV；同时保留季度结构化数据以备溯源。
- Section 指纹在 `chunk_index` 文件中以 `{section_name}:{序号}` 的形式标识，方便同名章节多次出现时分别追踪。

## 7. Pinecone 与向量策略
- n8n 使用 Google Gemini `models/gemini-embedding-001`（3072 维），对应 Pinecone `test-index-3072`；
- 清洗阶段控制 chunk 长度 400~600 汉字（≈800~900 tokens），保证嵌入稳定；
- 每条向量保留 `fund_code`, `section`, `quarter`, `chunk_hash` 等元数据，便于追踪；
- 如更换嵌入模型需同步调整 Pinecone 索引与工作流配置。

## 8. 项目结构与依赖规划
```
hsbc_data_cleaner/
├── cli.py                 # CLI 入口
├── config.py              # 配置加载
├── orchestrator.py        # 文件调度、管线控制
├── preprocessing/
│   ├── english_filter.py  # 英文页检测
│   └── loaders.py         # PDF 读取/写入
├── parsers/
│   ├── pdf_parser.py      # 章节/表格抽取
│   └── section_rules.py   # 章节匹配规则
├── cleaning/
│   ├── normalizers.py     # 文本清洗
│   └── deduplicate.py     # 指纹与相似度
├── chunking/
│   └── chunker.py         # 切片与摘要
├── outputs/
│   ├── writer_jsonl.py
│   ├── writer_structured.py
│   └── manifest.py        # processed_manifest & chunk_index
└── utils/
    ├── logging.py
    └── typing.py
```
- 运行依赖：`pdfplumber`, `pypdf`, `rapidfuzz`/`python-Levenshtein`, `numpy`, `pandas`, `pydantic`, `typer`, `orjson`；
- 开发依赖：`pytest`, `pytest-cov`, `ruff`/`black`, `mypy`（可选）。

## 9. 与 n8n 工作流的集成
- 清洗输出（JSONL/TXT）上传到 Google Drive “待处理”文件夹；
- 工作流调整：
  - 关闭或放大 `Recursive Character Text Splitter`（`chunkSize=4000`、`overlap=0`）；
  - `Default Data Loader` 读取清洗输出并保留 `metadata`；
  - `Set max chunks` 可保留 1000 的限制；
  - 日志、Telegram 节点保持不变；
- 去重逻辑留在清洗程序，n8n 只负责嵌入与写入。

## 10. 增量与去重策略
- **章节指纹**：规范化文本 → SHA256 指纹，与上一季度数据比对；
- **相似度阈值**：指纹不同但余弦相似度 > 0.98 时视为“轻微变化”，按业务决定是否生成新向量；
- **变化摘要**：利用结构化数据对比生成“新增/减少/变更”说明，并以 `summary` chunk 输出；
- **版本化元数据**：保留 `quarter`, `report_date`, `chunk_hash`, `previous_chunk_hash` 等字段；
- **manifest 维护**：记录每章节最新指纹、生成时间、引用关系，支持回溯。

## 11. 交付物
- 清洗程序源码与 CLI 使用说明；
- 样例输入/输出（原始 PDF、chunk JSONL、结构化 CSV）；
- 与 n8n/Pinecone 的联调示例与配置指引；
- 运维文档：调度、错误处理、manifest 更新流程、日志说明。

## 12. 运行与使用指引
1. **环境准备**
   - 建议 Python 3.10+，创建虚拟环境并安装依赖：
     ```bash
     python -m venv .venv
     source .venv/bin/activate
     pip install -r requirements.txt
     ```
   - 配置 `.env` 或 `config.toml`，指定 `RAW_DIR`, `CLEAN_DIR`, `STRUCTURED_DIR`, `DRIVE_FOLDER_ID` 等参数；本地与服务器可分别使用不同配置（如本地 `~/projects/hsbc/raw`、服务器 `/data/hsbc/raw`），程序依据运行环境读取对应路径。
2. **获取原始 PDF**
   - 将爬虫下载的季度 PDF 放置在默认目录 `raw/pdf/YYYY-Q/`，或在运行命令时通过 `--input-dir /path/to/folder` 指向自定义位置（支持本地磁盘、挂载的 NAS/NFS、或同步后的服务器目录）；
   - 若从 Drive 拉取原件，可使用脚本 `python -m hsbc_data_cleaner.sync --quarter 2025Q2 --mode pull --target /path/to/raw`（待实现），`--target` 同样可以是本地或远程挂载路径。
3. **执行清洗**
   - 命令示例：
     ```bash
     python -m hsbc_data_cleaner.cli \
       --quarter 2025Q2 \
       --fund-code U62717 \
       --input-dir /data/hsbc/raw/2025-Q2 \
       --incremental true
     ```
   - 关键参数：
     - `--quarter`：目标季度，必填；
     - `--fund-code`：可选，缺省时批量处理季度内全部基金；
     - `--input-dir`：可选，覆盖默认 `raw/pdf/YYYY-Q/`；
     - `--chunks-dir`：可选，覆盖默认 `clean/chunks/YYYY-Q/`（可指向本地或服务器挂载路径）；
     - `--incremental`：默认启用，仅处理新增/变更文件；设置为 `false` 可强制全量重跑；
     - `--upload`：启用后在清洗完成自动上传 chunk 到 Drive。
4. **检查输出**
   - 文本 chunk：`clean/chunks/YYYY-Q/`；结构化数据：`outputs/structured/YYYY-Q/`；
   - Manifest：`state/processed_manifest.json`、`state/chunk_index.json` 更新；
   - 日志：`logs/`（若启用文件日志）。
5. **上传至 Drive**
  - 若未在 CLI 中开启 `--upload`，可运行：
    ```bash
    python -m hsbc_data_cleaner.cli upload \
      --quarter 2025Q2 \
      --chunks-dir clean/chunks/2025-Q2 \
      --drive-folder 1PNFFxmkelrTRls98t3RH5AaQufL8V9GQ
    ```
  - `--chunks-dir` 支持传入任何可访问路径（本地、NAS、服务器挂载目录）；
  - 上传成功后会在 manifest 中写入 `uploaded_at` 字段，n8n 触发器将检测到新文件开始向量化。
6. **监控与回滚**
   - 通过 Google Sheets 日志与 Telegram 通知（n8n 节点）确认处理结果；
   - 如需回滚，可删除 `clean/chunks` 对应文件并更新 manifest，重新运行清洗。

## 13. 待进一步细化的事项
- 章节识别规则需覆盖不同基金系列的标题变体；
- 评估复杂表格解析精度，必要时引入 OCR/表格重构策略；
- 明确结构化数据字段及与 TXT 项目对接方式；
- 设计变化摘要的模板、阈值与多语言处理；
- 预研 Milvus 等本地向量库的兼容方案。

### 13.1 向量库中重复数据的影响讨论
- **潜在影响**：
  - 多个高度相同的向量会占用存储与调用配额，并可能在检索时返回重复内容；
  - 对于基于相似度的问答系统，重复向量会提高重复段落在 Top-K 中出现的概率，压缩其他多样化信息的曝光度；
  - 在 Pinecone 中，重复向量仍需付费存储，且写入/删除都会消耗资源。
- **缓解策略**：
  - 清洗阶段尽量做指纹去重，只对增量内容生成向量，从源头减少重复；
  - 若历史数据已写入，可定期运行去重脚本，比较 `chunk_hash` 并删除冗余向量；
  - 在检索阶段，可对返回结果做去重（依据 `chunk_hash` 或 `fund_code + section`），避免回答中重复引用同一段落；
  - 通过 `metadata` 区分不同版本（例如 `quarter`、`change_type`），让查询时可过滤出最新版本或特定区间的内容。
- **结论**：重复数据不会破坏检索正确性，但会降低检索多样性、增加资源开销；因此在清洗阶段控制增量、维护指纹缓存是最优选择，同时保留检索端去重与清理脚本的可能性。

## 14. 开发步骤与里程碑
为便于迭代开发与调试，建议按以下阶段逐步实现并在每阶段完成后进行自测：

1. **基础设施搭建**
   - 完成项目骨架、配置加载、CLI 框架；
   - 实现目录解析与参数覆盖（本地/远程路径），并编写最小化集成测试（如 `pytest` 针对配置模块）。

2. **英文页剔除模块**
   - 开发 `preprocessing/english_filter.py`，输入原始 PDF 输出剔除英文页的版本及记录；
  - 调试目标：使用一份样例 PDF 跑通剔除逻辑，核对日志与剔除页码；
   - 编写单元测试覆盖中文计数字段和边界情况（空页、混合页）。

3. **章节解析与结构化抽取**
   - 实现 `pdf_parser.py` 与 `section_rules.py`，完成章节分段、表格抽取；
   - 调试目标：对样例 PDF 输出章节文本和结构化表格 JSON；
   - 引入 fixtures 进行 parser 单元测试，确保标题匹配准确。

4. **标准化与清洗**
   - 开发 `cleaning/normalizers.py`，处理文本去噪、日期/币种规范；
   - 调试目标：确保规范化文本满足指纹计算要求，无冗余空格或重复行；
   - 测试：对关键正则、数值解析编写单元测试。

5. **指纹与差异检测**
   - 实现 `cleaning/deduplicate.py` 与 `state/chunk_index` 读写；
   - 调试目标：构造上一季度数据，验证 `reuse`/`updated` 判定及相似度阈值行为；
   - 测试：针对不同相似度场景编写单元用例。

6. **切片与变化摘要**
   - 开发 `chunking/chunker.py`，生成 chunk、处理 overlap、输出变化摘要；
   - 调试目标：检查 chunk 长度、元数据完整性以及 `summary` 生成逻辑；
   - 测试：对 chunk 边界、摘要输入输出编写测试。

7. **输出与上传**
   - 实现 `outputs/writer_jsonl.py`、`writer_structured.py`、`manifest.py`，以及 Drive 上传逻辑；
   - 调试目标：在本地输出 JSONL/CSV 后调用上传命令，确认 manifest 与状态文件更新正常；
   - 测试：使用临时目录/模拟 Drive API（可通过接口 mock）。

8. **端到端验证与集成**
   - 组合上述模块，执行 `python -m hsbc_data_cleaner.cli --quarter ... --upload`；
   - 检查 n8n 触发流程是否正常触发，并核对 Pinecone 向量数量；
   - 编写端到端测试脚本（可使用小型样例 PDF）以验证主要流程。

每个阶段完成后应更新 README 与变更日志，并在必要时同步调整文档中的流程示意图/配置说明。
