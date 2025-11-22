import os
from datetime import datetime
import json

# 定义目录和文件结构
structure = {
    "db": ["init_db.py", "models.py" , "session.py" ,"sample_data.py"],
    "intent": ["parser.py","schema.py"],
    "plan": ["builder.py"],
    "execution": ["executor.py"],
    "graph" :["dag_builder.py"],
    "utils" : ["llm_client.py","sqlglot_utils.py","risk_policy.py"],
    "audit": ["log_manager.py", "replay.py"],
    "runs": [],  # 文件稍后自动生成
}

# 根目录
base_dir = "poc"
os.makedirs(base_dir, exist_ok=True)

# 创建子目录和文件
for folder, files in structure.items():
    folder_path = os.path.join(base_dir, folder)
    os.makedirs(folder_path, exist_ok=True)
    for f in files:
        open(os.path.join(folder_path, f), "w").close()

# 创建 app.py 和 README.md
open(os.path.join(base_dir, "app.py"), "w").close()
open(os.path.join(base_dir, "README.md"), "w").close()
open(os.path.join(base_dir, "requirements.txt"), "w").close()
open(os.path.join(base_dir, ".env.example"), "w").close()

print(f"✅ Project structure created under '{base_dir}/'")
