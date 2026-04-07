#!/usr/bin/env python3
"""
AURA - Agente Horario: Input de Tarefas via Reuniao + Processamento de Exames
Usa Anthropic SDK para raciocinio IA e requests para chamar Notion/Slack diretamente.
"""

import os
import json
import requests
from datetime import datetime, timedelta
import anthropic

# --- Credenciais via env vars ---
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

NOTION_VERSION = "2022-06-28"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# --- IDs Notion ---
INBOX_PAGE_ID = "331f915e8d4f81278116efcb030aec6e"
TAREFAS_DB_ID = "31ff915e8d4f8011986bd55487330ecd"
ANAMNESE_DB_ID = "329f915e8d4f8040b4cf000be702925d"
CRM_COMFORT_ID = "23af915e8d4f80299d5e000b4ee52a40"
CRM_LUCAS_ID   = "292f915e8d4f80bf83ce000b2d85c44e"
CRM_LUANNA_ID  = "29cf915e8d4f802d8c71000baa52ea86"

OWNER_SLACK = {
    "Eduardo": "@Edu",
    "Lucas":   "@Lucas Mendonca",
    "Luanna":  "@Luanna Estebanez",
    "Bruna":   "@Bruna Mendonca",
}

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ============================================================
# Helpers Notion
# ============================================================

def notion_get(path):
    r = requests.get(f"https://api.notion.com/v1/{path}", headers=NOTION_HEADERS)
    r.raise_for_status()
    return r.json()

def notion_post(path, body):
    r = requests.post(f"https://api.notion.com/v1/{path}", headers=NOTION_HEADERS, json=body)
    r.raise_for_status()
    return r.json()

def notion_patch(path, body):
    r = requests.patch(f"https://api.notion.com/v1/{path}", headers=NOTION_HEADERS, json=body)
    r.raise_for_status()
    return r.json()

def get_all_blocks(page_id):
    """Busca todos os blocos de uma pagina, lidando com paginacao."""
    blocks = []
    cursor = None
    while True:
        url = f"blocks/{page_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={cursor}"
        data = notion_get(url)
        blocks.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return blocks

def extract_text_from_blocks(blocks):
    """Extrai todo o texto plano de uma lista de blocos Notion."""
    lines = []
    for b in blocks:
        btype = b.get("type", "")
        content = b.get(btype, {})
        rich = content.get("rich_text", [])
        text = "".join(r.get("plain_text", "") for r in rich)
        if text.strip():
            lines.append(text)
    return "\n".join(lines)

def get_page_title(page_id):
    try:
        page = notion_get(f"pages/{page_id}")
        props = page.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                rich = prop.get("title", [])
                return "".join(r.get("plain_text", "") for r in rich)
        # fallback: child_page title
        return page.get("child_page", {}).get("title", "Sem titulo")
    except Exception:
        return "Sem titulo"

def get_comments(page_id):
    try:
        data = notion_get(f"comments?block_id={page_id}")
        return data.get("results", [])
    except Exception:
        return []

def add_comment(page_id, text):
    notion_post("comments", {
        "parent": {"page_id": page_id},
        "rich_text": [{"text": {"content": text}}],
    })

def search_database(db_id, title_query):
    body = {"filter": {"property": "Tarefa", "title": {"contains": title_query}}}
    try:
        data = notion_post(f"databases/{db_id}/query", body)
        return data.get("results", [])
    except Exception:
        return []

def query_database(db_id, filter_body=None, page_size=50):
    body = {"page_size": page_size}
    if filter_body:
        body["filter"] = filter_body
    try:
        data = notion_post(f"databases/{db_id}/query", body)
        return data.get("results", [])
    except Exception:
        return []


# ============================================================
# Helper Slack
# ============================================================

def slack_notify(task_name, owner, prazo, context, notion_url):
    mention = OWNER_SLACK.get(owner, f"@{owner}")
    prazo_fmt = prazo if prazo else "Sem prazo"
    payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": "📋 NOVA TAREFA ATRIBUÍDA"}},
            {"type": "section", "text": {"type": "mrkdwn",
                "text": f"{mention}\n*{task_name}*\n• Owner: {owner}\n• Prazo: {prazo_fmt}\n• Status: Não iniciada\n_{context}_"}},
            {"type": "section", "text": {"type": "mrkdwn",
                "text": f"<{notion_url}|Ver tarefa no Notion>"}},
        ]
    }
    try:
        r = requests.post(SLACK_WEBHOOK, json=payload)
        r.raise_for_status()
    except Exception as e:
        print(f"  [AVISO] Slack falhou: {e}")


# ============================================================
# IA: extrair tarefas da reuniao
# ============================================================

def extract_tasks_with_ai(meeting_text, meeting_title):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    prompt = f"""Voce e um assistente da AURA Consultoria.
Analise o resumo/transcricao da reuniao abaixo e extraia TODOS os action items e tarefas.

Data de hoje: {today}
Titulo da reuniao: {meeting_title}

Mapeamento de owners:
- Lucas Mendonca -> Lucas
- Luanna Estebanez -> Luanna
- Bruna Mendonca -> Bruna
- Eduardo / Eduardo David / Eduardo Silveira -> Eduardo

Regras:
- Ignore tarefas ja concluidas ou apenas revisadas sem nova acao
- Se nao houver prazo explicito, use data_reuniao + 7 dias
- Se a data da reuniao nao estiver clara, use hoje + 7 dias como prazo
- Identifique o owner pelo contexto (quem vai fazer a tarefa)
- Se owner nao identificado, use Eduardo

Retorne SOMENTE um JSON valido no formato:
{{
  "meeting_date": "YYYY-MM-DD",
  "tasks": [
    {{
      "name": "Nome claro e objetivo da tarefa",
      "owner": "Eduardo|Lucas|Luanna|Bruna",
      "deadline": "YYYY-MM-DD",
      "summary": "Resumo do que fazer em 2-4 linhas",
      "attention_points": ["ponto 1", "ponto 2", "ponto 3"]
    }}
  ]
}}

REUNIAO:
{meeting_text[:8000]}
"""
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        content = message.content[0].text.strip()
        # Limpa markdown se necessario
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        return json.loads(content)
    except Exception as e:
        print(f"  [ERRO] IA falhou ao extrair tarefas: {e}")
        return {"meeting_date": today, "tasks": []}


# ============================================================
# Criar tarefa no Notion
# ============================================================

def create_task_in_notion(task, meeting_title, meeting_date):
    children = [
        {
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": task["summary"]}}]}
        },
        {
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": "Pontos de atenção"}}]}
        },
    ]
    for point in task.get("attention_points", []):
        children.append({
            "object": "block", "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": [{"text": {"content": point}}]}
        })
    children.append({
        "object": "block", "type": "paragraph",
        "paragraph": {"rich_text": [{"text": {"content": f"Originado da reunião: {meeting_title} — {meeting_date}"}}]}
    })

    # Formatar data para DD/MM/AAAA no Status
    try:
        d = datetime.strptime(task["deadline"], "%Y-%m-%d")
        deadline_display = d.strftime("%d/%m/%Y")
    except Exception:
        deadline_display = task["deadline"]

    body = {
        "parent": {"database_id": TAREFAS_DB_ID},
        "properties": {
            "Tarefa": {"title": [{"text": {"content": task["name"]}}]},
            "Owner": {"multi_select": [{"name": task["owner"]}]},
            "Status": {"select": {"name": "Não iniciada"}},
            "Data limite de entrega": {"date": {"start": task["deadline"]}},
            "Conferido": {"checkbox": False},
        },
        "children": children,
    }
    result = notion_post("pages", body)
    return result.get("url", "")


# ============================================================
# JOB 1: Input de Tarefas via Reuniao
# ============================================================

def job1_input_tarefas():
    print("\n=== JOB 1: Input de Tarefas via Reuniao ===")

    # 1.1 Buscar child_pages no INBOX REUNIOES
    try:
        blocks = get_all_blocks(INBOX_PAGE_ID)
    except Exception as e:
        print(f"  [ERRO] Nao foi possivel acessar o INBOX: {e}")
        return

    inbox_pages = [b for b in blocks if b.get("type") == "child_page"]
    if not inbox_pages:
        print("  Nenhuma pagina no INBOX REUNIOES.")
        return

    pages_to_process = []
    for page in inbox_pages:
        page_id = page["id"].replace("-", "")
        title = page.get("child_page", {}).get("title", "Sem titulo")

        # 1.2 Verificar comentarios
        comments = get_comments(page_id)
        already_processed = any(
            "Tarefas criadas" in "".join(r.get("plain_text", "") for r in c.get("rich_text", []))
            for c in comments
        )
        if already_processed:
            print(f"  [SKIP] '{title}' — ja processada.")
            continue
        pages_to_process.append({"id": page_id, "title": title})

    if not pages_to_process:
        print("  Nenhuma reuniao nova para processar.")
        return

    for page in pages_to_process:
        page_id = page["id"]
        title = page["title"]
        print(f"\n  Processando: {title}")

        # 1.3 Ler conteudo
        try:
            content_blocks = get_all_blocks(page_id)
            meeting_text = extract_text_from_blocks(content_blocks)
        except Exception as e:
            print(f"  [ERRO] Nao foi possivel ler conteudo: {e}")
            continue

        if not meeting_text.strip():
            print(f"  [SKIP] Pagina sem conteudo de texto.")
            continue

        # IA extrai tarefas
        result = extract_tasks_with_ai(meeting_text, title)
        tasks = result.get("tasks", [])
        meeting_date = result.get("meeting_date", datetime.utcnow().strftime("%Y-%m-%d"))

        if not tasks:
            print(f"  Nenhuma tarefa identificada.")
            add_comment(page_id, f"Tarefas criadas em {datetime.utcnow().strftime('%d/%m/%Y')} — 0 tarefa(s) identificada(s).")
            continue

        created_count = 0
        for task in tasks:
            task_name = task["name"]

            # 1.4 Verificar duplicatas
            existing = search_database(TAREFAS_DB_ID, task_name[:50])
            is_duplicate = False
            for item in existing:
                props = item.get("properties", {})
                # Checar owner
                owners = [o.get("name", "") for o in props.get("Owner", {}).get("multi_select", [])]
                status = props.get("Status", {}).get("select", {}).get("name", "")
                if task["owner"] in owners and status != "Concluída":
                    is_duplicate = True
                    break

            if is_duplicate:
                print(f"  [SKIP] Duplicata: '{task_name}'")
                continue

            # 1.5 Criar tarefa
            try:
                notion_url = create_task_in_notion(task, title, meeting_date)
                print(f"  [OK] Tarefa criada: '{task_name}' -> {task['owner']}")
                created_count += 1

                # 1.7 Notificar Slack
                try:
                    prazo_fmt = datetime.strptime(task["deadline"], "%Y-%m-%d").strftime("%d/%m/%Y")
                except Exception:
                    prazo_fmt = task["deadline"]
                context_text = task.get("summary", "")[:200]
                slack_notify(task_name, task["owner"], prazo_fmt, context_text, notion_url)
                print(f"  [OK] Slack notificado para '{task_name}'")

            except Exception as e:
                print(f"  [ERRO] Falhou ao criar tarefa '{task_name}': {e}")

        # 1.6 Comentar no INBOX
        today_str = datetime.utcnow().strftime("%d/%m/%Y")
        add_comment(page_id, f"Tarefas criadas em {today_str} — {created_count} tarefa(s) adicionada(s) ao Controle de Tarefas.")
        print(f"  Comentario adicionado na pagina '{title}'.")


# ============================================================
# JOB 2: Processamento de Exames Laboratoriais
# ============================================================

def job2_processamento_exames():
    print("\n=== JOB 2: Processamento de Exames Laboratoriais ===")

    # 2.1 Buscar formularios de anamnese
    try:
        results = query_database(ANAMNESE_DB_ID)
    except Exception as e:
        print(f"  [ERRO] Nao foi possivel acessar base de anamnese: {e}")
        return

    pendentes = []
    for entry in results:
        entry_id = entry["id"].replace("-", "")
        # Verificar se tem arquivo de exames
        blocks = get_all_blocks(entry_id)
        file_url = None
        for b in blocks:
            btype = b.get("type", "")
            if btype in ("file", "pdf"):
                file_info = b.get(btype, {})
                file_url = file_info.get("file", {}).get("url") or file_info.get("external", {}).get("url")
                if file_url:
                    break

        if not file_url:
            continue

        # Extrair nome do cliente
        props = entry.get("properties", {})
        nome = ""
        for prop in props.values():
            if prop.get("type") == "title":
                nome = "".join(r.get("plain_text", "") for r in prop.get("title", []))
                break

        if not nome:
            continue

        # Buscar card no CRM
        card = None
        card_db = None
        for db_id in [CRM_COMFORT_ID, CRM_LUCAS_ID, CRM_LUANNA_ID]:
            results_crm = query_database(db_id, {
                "property": "Name", "title": {"contains": nome.split()[0]}
            })
            if results_crm:
                card = results_crm[0]
                card_db = db_id
                break

        if not card:
            print(f"  [SKIP] Card nao encontrado no CRM para: {nome}")
            continue

        card_id = card["id"].replace("-", "")

        # Verificar se ja tem subpagina de exames
        card_blocks = get_all_blocks(card_id)
        has_exam_page = any(
            "Exames Laboratoriais" in b.get("child_page", {}).get("title", "")
            for b in card_blocks if b.get("type") == "child_page"
        )
        if has_exam_page:
            print(f"  [SKIP] {nome} ja tem subpagina de exames.")
            continue

        pendentes.append({
            "nome": nome,
            "file_url": file_url,
            "card_id": card_id,
            "entry_id": entry_id,
        })

    if not pendentes:
        print("  Nenhum exame pendente.")
        return

    for item in pendentes:
        print(f"\n  Processando exames: {item['nome']}")
        try:
            # 2.2 Ler PDF
            pdf_response = requests.get(item["file_url"], timeout=30)
            pdf_response.raise_for_status()
            pdf_text = pdf_response.text[:10000]  # Limitar tamanho

            # IA classifica exames
            today_str = datetime.utcnow().strftime("%d/%m/%Y")
            prompt = f"""Voce e um nutricionista/medico analisando exames laboratoriais.

Paciente: {item['nome']}
Data: {today_str}

Extraia e classifique os exames do texto abaixo usando diretrizes internacionais (ADA 2024, Endocrine Society, AHA/ACC, SBC, WHO, KDIGO, ATA, AASLD, Tietz).

Classificacoes: Verde=normal, Amarelo=limitrofe, Vermelho=alterado

Retorne JSON:
{{
  "resumo_analitico": "Resumo geral dos achados em 3-5 linhas",
  "categorias": {{
    "HEMOGRAMA": [{{"exame": "", "valor": "", "referencia": "", "classificacao": "verde|amarelo|vermelho"}}],
    "GLICEMIA E METABOLICO": [],
    "VITAMINAS E MINERAIS": [],
    "METABOLISMO DO FERRO": [],
    "HORMONIOS SEXUAIS": [],
    "LIPIDIOS": [],
    "HEPATICO": [],
    "RENAL": [],
    "TIREOIDE": []
  }}
}}

TEXTO DOS EXAMES:
{pdf_text}
"""
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=3000,
                messages=[{"role": "user", "content": prompt}]
            )
            content = message.content[0].text.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            exam_data = json.loads(content)

            # 2.4 Criar subpagina no CRM
            title_page = f"Exames Laboratoriais — {item['nome']} — {today_str}"
            children = [
                {
                    "object": "block", "type": "callout",
                    "callout": {
                        "rich_text": [{"text": {"content": exam_data.get("resumo_analitico", "")}}],
                        "color": "yellow_background",
                        "icon": {"type": "emoji", "emoji": "⚠️"}
                    }
                }
            ]

            categorias = exam_data.get("categorias", {})
            ordem = ["HEMOGRAMA", "GLICEMIA E METABOLICO", "VITAMINAS E MINERAIS",
                     "METABOLISMO DO FERRO", "HORMONIOS SEXUAIS", "LIPIDIOS",
                     "HEPATICO", "RENAL", "TIREOIDE"]

            for cat in ordem:
                exames = categorias.get(cat, [])
                children.append({"object": "block", "type": "divider", "divider": {}})
                children.append({
                    "object": "block", "type": "heading_2",
                    "heading_2": {"rich_text": [{"text": {"content": cat}}]}
                })
                if exames:
                    # Tabela simples como paragrafos
                    for ex in exames:
                        cor = {"verde": "✅", "amarelo": "⚠️", "vermelho": "🔴"}.get(
                            ex.get("classificacao", "").lower(), "•"
                        )
                        line = f"{cor} {ex.get('exame','')} — {ex.get('valor','')} (Ref: {ex.get('referencia','')})"
                        children.append({
                            "object": "block", "type": "paragraph",
                            "paragraph": {"rich_text": [{"text": {"content": line}}]}
                        })

            page_body = {
                "parent": {"page_id": item["card_id"]},
                "properties": {"title": {"title": [{"text": {"content": title_page}}]}},
                "icon": {"type": "emoji", "emoji": "🧪"},
                "children": children[:100],  # Notion limita 100 blocos por request
            }
            result = notion_post("pages", page_body)
            subpage_url = result.get("url", "")
            print(f"  [OK] Subpagina criada: {subpage_url}")

            # 2.5 Comentar no card do CRM
            add_comment(item["card_id"], f"Exames processados em {today_str} — {subpage_url}")
            print(f"  [OK] Comentario adicionado no card.")

        except Exception as e:
            print(f"  [ERRO] Falhou ao processar exames de {item['nome']}: {e}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print(f"AURA Agente Horario — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    job1_input_tarefas()
    job2_processamento_exames()
    print("\nAgente finalizado.")
