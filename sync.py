import os
import json
import logging
from datetime import datetime, timezone, timedelta

from notion_client import Client as NotionClient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_LISTA_GERAL = "5a144ec2998e4eadb0ce1e0edc4d1953"
DB_ROTINAS_SP  = "35f89cebf7bb48049c5459c4ad2a91c7"
TASKLIST_ID    = "MTIwOTg3MDI5MjMyMDQ3MjcxOTA6MDow"

STATUS_CONCLUIDO  = {DB_LISTA_GERAL: "Concluído",  DB_ROTINAS_SP: "Concluido"}
STATUS_CANCELADA  = "Cancelada"
TERMINAL_STATUSES = {
    DB_LISTA_GERAL: {"Concluído", "Cancelada"},
    DB_ROTINAS_SP:  {"Concluido", "Cancelada"},
}

SYNC_WINDOW_MIN   = 20
UPDATE_WINDOW_MIN = 60

_title_key_cache = {}


def build_google_tasks_service():
    creds_data = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
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
    for prop in page["properties"].values():
        if prop.get("type") == "title":
            return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    return ""


def get_title_key(notion, db_id):
    if db_id not in _title_key_cache:
        db = notion.databases.retrieve(database_id=db_id)
        key = ""
        for k, prop in db.get("properties", {}).items():
            if prop.get("type") == "title":
                key = k
                break
        _title_key_cache[db_id] = key
        log.info("  Title key DB %s: '%s'", db_id, key)
    return _title_key_cache[db_id]


def get_text(page, field):
    prop = page["properties"].get(field, {})
    ptype = prop.get("type")
    return "".join(t.get("plain_text", "") for t in prop.get(ptype or "", []))


def get_date(page, field):
    d = page["properties"].get(field, {}).get("date")
    return d["start"][:10] if d and d.get("start") else None


def get_status(page):
    sel = page["properties"].get("Status", {}).get("select")
    return sel["name"] if sel else None


def get_google_id(page):
    return get_text(page, "Google ID")


def page_url(page):
    return "https://www.notion.so/" + page["id"].replace("-", "")


def to_google_due(date_str):
    return date_str + "T00:00:00.000Z" if date_str else None


def from_google_due(due_str):
    return due_str[:10] if due_str else None


def query_all_pages(notion, db_id, filter_obj):
    pages, cursor = [], None
    while True:
        params = {"database_id": db_id, "filter": filter_obj, "page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        resp = notion.databases.query(**params)
        pages.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return pages


def get_all_notion_google_ids(notion):
    mapping = {}
    for db_id in [DB_LISTA_GERAL, DB_ROTINAS_SP]:
        for page in query_all_pages(notion, db_id, {
            "property": "Google ID", "rich_text": {"is_not_empty": True}
        }):
            gid = get_google_id(page)
            if gid:
                mapping[gid] = (page["id"], db_id)
    return mapping


def fetch_google_task(svc, task_id):
    try:
        return svc.tasks().get(tasklist=TASKLIST_ID, task=task_id).execute()
    except HttpError as e:
        if e.resp.status == 404:
            return None
        raise


def get_all_active_google_tasks(svc):
    tasks, page_token = [], None
    while True:
        params = {"tasklist": TASKLIST_ID, "showCompleted": False, "showHidden": False, "maxResults": 100}
        if page_token:
            params["pageToken"] = page_token
        resp = svc.tasks().list(**params).execute()
        tasks.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return tasks


def sync_notion_to_google(notion, svc):
    log.info("=== Notion -> Google Tasks ===")
    created = updated = cancelled = 0

    for db_id in [DB_LISTA_GERAL, DB_ROTINAS_SP]:
        terminal   = TERMINAL_STATUSES[db_id]
        status_done = STATUS_CONCLUIDO[db_id]

        # 1. CREATE: novos sem Google ID e status nao terminal
        new_pages = query_all_pages(notion, db_id, {
            "and": [
                {"property": "Google ID", "rich_text": {"is_empty": True}},
                {"property": "Status", "select": {"does_not_equal": status_done}},
                {"property": "Status", "select": {"does_not_equal": STATUS_CANCELADA}},
            ]
        })
        log.info("  [%s] novas: %d", db_id, len(new_pages))
        for page in new_pages:
            title = get_title(page)
            if not title:
                continue
            date_str = get_date(page, "Prazo final") or get_date(page, "Data")
            body = {"title": title, "notes": page_url(page)}
            if date_str:
                body["due"] = to_google_due(date_str)
            try:
                task = svc.tasks().insert(tasklist=TASKLIST_ID, body=body).execute()
                notion.pages.update(
                    page_id=page["id"],
                    properties={
                        "Google ID": {"rich_text": [{"text": {"content": task["id"]}}]},
                        "Fonte": {"select": {"name": "Notion"}},
                    }
                )
                log.info("  Criado: '%s'", title)
                created += 1
            except Exception as e:
                log.error("  Erro criar '%s': %s", title, e)

        # 2. CANCEL: com Google ID e Status=Cancelada -> deleta do Google
        cancel_pages = query_all_pages(notion, db_id, {
            "and": [
                {"property": "Google ID", "rich_text": {"is_not_empty": True}},
                {"property": "Status", "select": {"equals": STATUS_CANCELADA}},
            ]
        })
        for page in cancel_pages:
            gid   = get_google_id(page)
            title = get_title(page)
            try:
                task = fetch_google_task(svc, gid)
                if task and task.get("status") != "completed":
                    svc.tasks().delete(tasklist=TASKLIST_ID, task=gid).execute()
                notion.pages.update(page_id=page["id"], properties={"Google ID": {"rich_text": []}})
                log.info("  Cancelada, removida do Google: '%s'", title)
                cancelled += 1
            except Exception as e:
                log.error("  Erro cancelar '%s': %s", title, e)

        # 3. UPDATE: editadas no Notion nos ultimos UPDATE_WINDOW_MIN
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=UPDATE_WINDOW_MIN)).isoformat()
        try:
            update_pages = query_all_pages(notion, db_id, {
                "and": [
                    {"property": "Google ID", "rich_text": {"is_not_empty": True}},
                    {"timestamp": "last_edited_time", "last_edited_time": {"after": cutoff}},
                ]
            })
        except Exception as exc:
            log.warning("  Filtro timestamp nao suportado (%s), pulando update", exc)
            update_pages = []

        for page in update_pages:
            if get_status(page) in terminal:
                continue
            gid   = get_google_id(page)
            title = get_title(page)
            task  = fetch_google_task(svc, gid)
            if task is None:
                notion.pages.update(page_id=page["id"], properties={
                    "Google ID": {"rich_text": []},
                    "Status":    {"select": {"name": STATUS_CANCELADA}},
                })
                log.info("  Task deletada no Google, marcada Cancelada no Notion: '%s'", title)
                continue
            if task.get("status") == "completed":
                continue
            patch, changed = {}, False
            if task.get("title") != title:
                patch["title"] = title
                changed = True
            notion_date = get_date(page, "Prazo final") or get_date(page, "Data")
            google_date = from_google_due(task.get("due"))
            if notion_date != google_date:
                patch["due"] = to_google_due(notion_date)
                changed = True
            if changed:
                try:
                    svc.tasks().patch(tasklist=TASKLIST_ID, task=gid, body=patch).execute()
                    log.info("  Atualizado no Google: '%s'", title)
                    updated += 1
                except Exception as e:
                    log.error("  Erro atualizar '%s': %s", title, e)

    log.info("  Criados: %d | Atualizados: %d | Cancelados: %d", created, updated, cancelled)


def sync_google_to_notion(notion, svc):
    log.info("=== Google Tasks -> Notion ===")
    notion_ids = get_all_notion_google_ids(notion)
    completed_n = created_n = 0

    # 1. COMPLETE: concluidas no Google nos ultimos SYNC_WINDOW_MIN
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=SYNC_WINDOW_MIN)).isoformat()
    try:
        resp = svc.tasks().list(
            tasklist=TASKLIST_ID, showCompleted=True, showHidden=True,
            completedMin=cutoff, maxResults=100
        ).execute()
        completed_tasks = [t for t in resp.get("items", []) if t.get("status") == "completed"]
    except Exception as e:
        log.error("Erro listar concluidas: %s", e)
        completed_tasks = []

    log.info("  Concluidas nos ultimos %dmin: %d", SYNC_WINDOW_MIN, len(completed_tasks))
    for task in completed_tasks:
        gid = task["id"]
        if gid not in notion_ids:
            log.info("  Sem pagina Notion para '%s', ignorando.", task.get("title", ""))
            continue
        page_id, db_id = notion_ids[gid]
        try:
            notion.pages.update(
                page_id=page_id,
                properties={"Status": {"select": {"name": STATUS_CONCLUIDO[db_id]}}}
            )
            log.info("  Concluido no Notion: '%s'", task.get("title", ""))
            completed_n += 1
        except Exception as e:
            log.error("  Erro concluir Notion '%s': %s", task.get("title"), e)

    # 2. CREATE: tasks do Google sem pagina no Notion
    try:
        active_tasks = get_all_active_google_tasks(svc)
    except Exception as e:
        log.error("Erro listar tasks ativas: %s", e)
        active_tasks = []

    new_tasks = [t for t in active_tasks if t["id"] not in notion_ids]
    log.info("  Tasks Google sem Notion: %d", len(new_tasks))

    title_key = get_title_key(notion, DB_LISTA_GERAL)
    for task in new_tasks:
        title = (task.get("title") or "").strip()
        if not title:
            continue
        date_str = from_google_due(task.get("due"))
        props = {
            title_key: {"title": [{"text": {"content": title}}]},
            "Google ID": {"rich_text": [{"text": {"content": task["id"]}}]},
            "Fonte": {"select": {"name": "Google Tasks"}},
            "Status": {"select": {"name": "Para fazer"}},
        }
        if date_str:
            props["Prazo final"] = {"date": {"start": date_str}}
        try:
            notion.pages.create(parent={"database_id": DB_LISTA_GERAL}, properties=props)
            log.info("  Criado no Notion: '%s'", title)
            created_n += 1
        except Exception as e:
            log.error("  Erro criar no Notion '%s': %s", title, e)

    log.info("  Concluidos no Notion: %d | Criados no Notion: %d", completed_n, created_n)


def main():
    if not os.environ.get("NOTION_TOKEN"):
        raise RuntimeError("NOTION_TOKEN nao definido.")
    if not os.environ.get("GOOGLE_CREDENTIALS_JSON"):
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON nao definido.")
    notion = NotionClient(auth=os.environ["NOTION_TOKEN"])
    svc    = build_google_tasks_service()
    sync_notion_to_google(notion, svc)
    sync_google_to_notion(notion, svc)
    log.info("=== Sync concluido ===")


if __name__ == "__main__":
    main()

