from pathlib import Path
import os

IS_IN_DOCKER = os.environ.get('DOCKER_DEPLOYMENT', False)

if os.name == 'nt':
    STORAGE_PATH = Path(".gerev") / "storage"
else:
    if IS_IN_DOCKER:
        STORAGE_PATH = Path('/opt/storage/')
    else:
        user_home = str(Path.home())
        STORAGE_PATH = Path(user_home) / ".gerev" / "storage"

if not STORAGE_PATH.exists():
    STORAGE_PATH.mkdir(parents=True, exist_ok=True)

UI_PATH = Path('/ui/') if IS_IN_DOCKER else Path('../ui/build/')
SQLITE_DB_PATH = STORAGE_PATH / 'db.sqlite3'
SQLITE_TASKS_PATH = STORAGE_PATH / 'tasks.sqlite3'
SQLITE_INDEXING_PATH = STORAGE_PATH / 'indexing.sqlite3'
FAISS_INDEX_PATH = str(STORAGE_PATH / 'faiss_index.bin')
BM25_INDEX_PATH = str(STORAGE_PATH / 'bm25_index.bin')
UUID_PATH = str(STORAGE_PATH / '.uuid')
