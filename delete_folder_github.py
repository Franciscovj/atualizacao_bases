import requests, base64, json, os, logging
from config import GITHUB_TOKEN_BET2ALL

# ---------------------------------------------------------
# CONFIGURAÇÕES
# ---------------------------------------------------------
OWNER = "bet2all"               
REPO = "Bet2All"                   
FOLDER_TO_DELETE = "data/season_2025"  
BRANCH = "main"                  

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/github_delete.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ---------------------------------------------------------
# CONFIGURAÇÃO GERAL
# ---------------------------------------------------------
API_BASE = "https://api.github.com"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN_BET2ALL}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "delete-folder-script"
}


# ---------------------------------------------------------
# FUNÇÕES AUXILIARES
# ---------------------------------------------------------
def github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"{API_BASE}/repos/{owner}/{repo}/contents/{path}"


def delete_file(owner: str, repo: str, path: str, sha: str, branch: str | None = None):
    """Apaga um arquivo individual do repositório."""
    url = github_contents_url(owner, repo, path)
    payload = {
        "message": f"Removendo {path} via script",
        "sha": sha
    }
    if branch:
        payload["branch"] = branch

    r = requests.delete(url, headers=HEADERS, data=json.dumps(payload))
    if r.status_code in (200, 204):
        logging.info(f"[OK] Apagado: {path}")
        print(f"[OK] Apagado: {path}")
    else:
        logging.error(f"[ERRO DELETE {r.status_code}] {path} -> {r.text}")
        print(f"[ERRO DELETE {r.status_code}] {path} -> {r.text}")


def delete_folder(owner: str, repo: str, folder_path: str, branch: str | None = None):
    """Apaga todos os arquivos e subpastas dentro da pasta especificada."""
    url = github_contents_url(owner, repo, folder_path)
    if branch:
        url = f"{url}?ref={branch}"

    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        print(f"[ERRO] Falha ao listar pasta {folder_path}: {r.status_code} -> {r.text}")
        logging.error(f"Falha ao listar pasta {folder_path}: {r.status_code} -> {r.text}")
        return

    items = r.json()
    if not isinstance(items, list):
        # Se for um único arquivo
        items = [items]

    for item in items:
        path = item["path"]
        sha = item["sha"]
        tipo = item["type"]

        if tipo == "dir":
            delete_folder(owner, repo, path, branch)
        else:
            delete_file(owner, repo, path, sha, branch)


# ---------------------------------------------------------
# EXECUÇÃO PRINCIPAL
# ---------------------------------------------------------
def main():
    print(f"🧹 Iniciando remoção da pasta: {FOLDER_TO_DELETE}")
    logging.info(f"Iniciando remoção da pasta {FOLDER_TO_DELETE}")
    delete_folder(OWNER, REPO, FOLDER_TO_DELETE, BRANCH)
    print("✅ Remoção concluída.")
    logging.info("Remoção concluída.")


if __name__ == "__main__":
    main()
