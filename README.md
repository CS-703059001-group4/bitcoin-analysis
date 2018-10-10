### 環境需求

* Docker
* Makefile

### 指令

```bash
# 1. 建立 virtual envirement
# 2. 啟動 spark cluster
# 3. 進入 virtual environment
$ make

# 執行 pyspark
$ make shell

# 執行程式 example.py (檔案名稱可改，不指定會執行 main.py)
$ make submit FILE=example.py

# 刪除所有由程式建立的檔案、container
$ make clean
```
