import os
import json
import logging
from datetime import datetime, timezone, timedelta

from notion_client import Client as NotionClient
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Constantes
DB_LISTA_GERAL = "5a144ec2998e4eadb0ce1e0edc4d1953"
DB_ROTINAS_SP  = "35f89cebf7bb48049c5459c4ad2a91c7"
TASKLIST_ID    = "MTIwOTg3MDI5MjMyMDQ3MjcxOTA6MDow"

STATUS_CONCLUIDO = {
    DB_LISTA_GERAL: "Concluído",
    DB_ROTINAS_SP:  "Concluido",
}



def build_google_tasks_service():
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    creds_data = json.loads(creds_json)
    creds = Credentials(
        token=creds_data.get("token"),
        refresh_token=creds_data["refresh_token"],
        token_uri=creds_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=creds_data["client_id"],
        client_secret=creds_data["client_secret"],
        scopes=creds_data.get("scopes", ["https://www.googleapis.com/auth/tasks"]),
    )
    if creds.expired or not creds.valid:
        creds.refresh(Request())
        log.info("Token Google renovado.")
    return build("tasks", "v1", credentials=creds, cache_discovery=False)


def get_title(page):
    """Retorna o título da página buscando pela propriedade de tipo 'title'."""
    for prop in page["properties"].values():
        if prop.get("type") == "title":
            return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    return ""


def get_text(page, field):
    prop = page["properties"].get(field, {})
    ptype = prop.get("type")
    items = prop.get(ptype, [])
    return "".join(t.get("plain_text", "") for t in items)


def get_date(page, field):
    prop = page["properties"].get(field, {})
    date_obj = prop.get("date")
    if date_obj and date_obj.get("start"):
        return date_obj["start"][:10]
    return None


def get_status(page):
    prop = page["properties"].get("Status", {})
    sel = prop.get("select")
    return sel["name"] if sel else None


def page_url(page):
    return "https://www.notion.so/" + page["id"].replace("-", "")


def sync_notion_to_google(notion, svc):
    log.info("=== Notion -> Google Tasks ===")
    total = 0
    for db_id in [DB_LISTA_GERAL, DB_ROTINAS_SP]:
        status_concluido = STATUS_CONCLUIDO[db_id]
        resp = notion.databases.query(
            database_id=db_id,
            filter={"property": "Google ID", "rich_text": {"is_empty": True}}
        )
        pages = resp.get("results", [])
        log.info("  DB %s: %d paginas sem Google ID (has_more=%s)", db_id, len(pages), resp.get("has_more"))
        for page in pages:
            status = get_status(page)
            title = get_title(page)
            log.info("    Pagina: '%s' | Status: %s | Google ID vazio: %s", title or "[sem titulo]", status, not get_text(page, "Google ID"))
            if status == status_concluido:
                continue
            if not title:
                continue
            date_str = get_date(page, "Prazo final") or get_date(page, "Data")
            due = (date_str + "T00:00:00.000Z") if date_str else None
            task_body = {"title": title, "notes": page_url(page)}
            if due:
                task_body["due"] = due
            try:
                created = svc.tasks().insert(tasklist=TASKLIST_ID, body=task_body).execute()
                google_task_id = created["id"]
                notion.pages.update(
                    page_id=page["id"],
                    properties={
                        "Google ID": {"rich_text": [{"text": {"content": google_task_id}}]},
                        "Fonte": {"select": {"name": "Notion"}}
                    }
                )
                log.info("  Criado: %s (ID: %s)", title, google_task_id)
                total += 1
            except Exception as e:
                log.error("  Erro ao criar task '%s': %s", title, e)
    log.info("  Total criado: %d task(s)", total)


def find_notion_page_by_google_id(notion, google_id):
    for db_id in [DB_LISTA_GERAL, DB_ROTINAS_SP]:
        try:
            resp = notion.databases.query(
                database_id=db_id,
                filter={"property": "Google ID", "rich_text": {"equals": google_id}}
            )
            results = resp.get("results", [])
            if results:
                return results[0]
        except Exception as e:
            log.error("Erro ao buscar Notion (db %s): %s", db_id, e)
    return None


def sync_google_to_notion(notion, svc):
    log.info("=== Google Tasks -> Notion ===")
    window_minutes = 20
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat()
    try:
        resp = svc.tasks().list(
            tasklist=TASKLIST_ID,
            showCompleted=True,
            showHidden=True,
            completedMin=cutoff,
            maxResults=100
        ).execute()
    except Exception as e:
        log.error("Erro ao listar tasks do Google: %s", e)
        return
    tasks = [t for t in resp.get("items", []) if t.get("status") == "completed"]
    log.info("  Tasks concluidas nos ultimos %dmin: %d", window_minutes, len(tasks))
    for task in tasks:
        google_id = task["id"]
        title     = task.get("title", "")
        page = find_notion_page_by_google_id(notion, google_id)
        if not page:
            log.info("  Sem pagina Notion para '%s' — ignorando.", title)
            continue
        db_id = page["parent"]["database_id"].replace("-", "")
        status_value = STATUS_CONCLUIDO.get(db_id)
        if not status_value:
            log.warning("  Database desconhecida: %s", db_id)
            continue
        current_status = get_status(page)
        if current_status == status_value:
            log.info("  '%s' ja esta concluida no Notion.", title)
            continue
        try:
            notion.pages.update(
                page_id=page["id"],
                properties={"Status": {"select": {"name": status_value}}}
            )
            log.info("  Notion atualizado -> Concluido: '%s'", title)
        except Exception as e:
            log.error("  Erro ao atualizar Notion para '%s': %s", title, e)


def main():
    notion_token = os.environ.get("NOTION_TOKEN")
    google_creds = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not notion_token:
        raise RuntimeError("NOTION_TOKEN nao definido.")
    if not google_creds:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON nao definido.")
    notion = NotionClient(auth=notion_token)
    svc    = build_google_tasks_service()
    sync_notion_to_google(notion, svc)
    sync_google_to_notion(notion, svc)
    log.info("=== Sync concluido ===")


if __name__ == "__main__":
    main()

