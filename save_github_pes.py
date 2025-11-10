import requests, base64, json, os, logging
from config import GITHUB_TOKEN_SKE

# Configura logging
logging.basicConfig(
    filename='logs/github_upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
OWNER = "Skedaddle3678"
REPO = "data"
"""
Atualizado para processar todos os CSVs dentro de `season_2025/` e enviá-los
individualmente para o repositório GitHub, preservando a hierarquia de pastas
em `data/season_2025/`.
"""

# Garante que a pasta de logs exista
os.makedirs(os.path.dirname('logs/github_upload.log'), exist_ok=True)

API_BASE = "https://api.github.com"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN_SKE}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "clear-sportmonks-script"
}

# Diretórios base
BASE_LOCAL_DIR = os.path.join(os.path.dirname(__file__), 'season_2025_pes')
BASE_REPO_DIR = 'data/season_2025_pes'  # Caminho no repositório GitHub


def github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"{API_BASE}/repos/{owner}/{repo}/contents/{path}"


def github_repo_url(owner: str, repo: str) -> str:
    return f"{API_BASE}/repos/{owner}/{repo}"
def get_remote_file_sha_and_lines(owner: str, repo: str, repo_path: str, branch: str | None = None):
    """Obtém SHA e número de linhas do arquivo remoto (se existir)."""
    url = github_contents_url(owner, repo, repo_path)
    if branch:
        url = f"{url}?ref={branch}"
    logging.info(f"[GET] Buscando arquivo no GitHub: {url}")
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        data = r.json()
        sha = data.get("sha")
        content_b64 = data.get("content", "")
        try:
            decoded = base64.b64decode(content_b64).decode()
        except Exception:
            decoded = ""
        linhas = len(decoded.strip().split('\n')) if decoded else 0
        logging.info(f"Encontrado no GitHub. SHA: {sha}, linhas: {linhas}")
        return sha, linhas, None
    elif r.status_code == 404:
        logging.warning("Arquivo não existe no repositório (404) — será criado.")
        return None, 0, None
    else:
        logging.error(f"Falha ao buscar arquivo remoto: status {r.status_code}, resp: {r.text}")
        return None, 0, r


def put_file(owner: str, repo: str, repo_path: str, content_str: str, sha: str | None, commit_message: str, branch: str | None = None, is_binary: bool = False):
    """Cria/atualiza arquivo via GitHub Contents API."""
    url = github_contents_url(owner, repo, repo_path)
    if is_binary:
        # Conteúdo já está codificado em base64
        content_b64 = content_str
    else:
        # Codificar texto em base64
        content_b64 = base64.b64encode(content_str.encode('utf-8')).decode('utf-8')
    payload = {"message": commit_message, "content": content_b64}
    if sha:
        payload["sha"] = sha
    if branch:
        payload["branch"] = branch
    logging.info(f"[PUT] Enviando arquivo: {repo_path} (sha: {sha})")
    r = requests.put(url, headers=HEADERS, data=json.dumps(payload))
    return r


def process_file(local_file_path: str) -> None:
    # Caminho relativo a partir da base local
    rel_local = os.path.relpath(local_file_path, BASE_LOCAL_DIR)
    # Converte para caminho de repo usando '/'
    repo_path = f"{BASE_REPO_DIR}/{rel_local.replace(os.sep, '/')}"

    try:
        # Lê conteúdo local (binário para Excel, texto para CSV)
        if local_file_path.lower().endswith('.xlsx'):
            with open(local_file_path, 'rb') as f:
                content_bytes = f.read()
            content = base64.b64encode(content_bytes).decode('utf-8')
            local_lines = "N/A (Excel)"
        else:
            with open(local_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            local_lines = len(content.splitlines())
        logging.info(f"Lendo arquivo local: {local_file_path} ({local_lines} linhas)")

        # Busca remoto
        sha, remote_lines, err = get_remote_file_sha_and_lines(OWNER, REPO, repo_path, None)
        if err is not None:
            logging.error(f"Abortando arquivo devido a erro no GET: {local_file_path}")
            try:
                err_body = err.text
            except Exception:
                err_body = ''
            print(f"[ERRO GET] {local_file_path} -> status {err.status_code} | resp: {err_body}")
            return

        # Monta mensagem de commit
        commit_message = f"Atualizando {repo_path} via script"

        # PUT (cria/atualiza)
        r = put_file(OWNER, REPO, repo_path, content, sha, commit_message, None, local_file_path.lower().endswith('.xlsx'))
        if r.status_code in (200, 201):
            resp = r.json()
            new_sha = resp.get('content', {}).get('sha')
            logging.info(f"Sucesso {r.status_code}: {repo_path} | SHA novo: {new_sha} | linhas local/remoto: {local_lines}/{remote_lines}")
            print(f"[OK {r.status_code}] {repo_path} -> linhas local/remoto: {local_lines}/{remote_lines}")
        else:
            logging.error(f"Erro no upload ({r.status_code}) para {repo_path}: {r.text}")
            # Diagnóstico extra para 403
            if r.status_code == 403:
                limit = r.headers.get('X-RateLimit-Remaining')
                scope = r.headers.get('X-OAuth-Scopes')
                res_scope = r.headers.get('X-Accepted-OAuth-Scopes')
                logging.error(f"403 detalhes -> Remaining: {limit}, Token scopes: {scope}, Required scopes: {res_scope}")
                print(f"[DICA 403] Verifique permissões do token (contents:write), autorização SSO na organização e se a branch main está protegida (requer PR).")
            # Exibir corpo do erro para diagnóstico
            body = r.text
            if body and len(body) > 1000:
                body = body[:1000] + '...'
            print(f"[ERRO PUT {r.status_code}] {repo_path} -> resp: {body}")
    except Exception as e:
        logging.exception(f"Exceção ao processar {local_file_path}: {e}")
        print(f"[EXCEPTION] {local_file_path}: {e}")


def main():
    if not os.path.isdir(BASE_LOCAL_DIR):
        logging.error(f"Diretório base não encontrado: {BASE_LOCAL_DIR}")
        print(f"Diretório base não encontrado: {BASE_LOCAL_DIR}")
        return

    total_files = 0
    processed = 0

    logging.info(f"Iniciando processamento dos arquivos em: {BASE_LOCAL_DIR}")
    for root, _, files in os.walk(BASE_LOCAL_DIR):
        for name in files:
            if not (name.lower().endswith('.csv') or name.lower().endswith('.xlsx')):
                continue
            total_files += 1
            local_path = os.path.join(root, name)
            logging.info(f"Processando arquivo: {local_path}")
            process_file(local_path)
            processed += 1

    logging.info(f"Processamento concluído. Arquivos encontrados: {total_files}, processados: {processed}")
    print(f"Concluído. Arquivos encontrados: {total_files}, processados: {processed}")


if __name__ == "__main__":
    main()
