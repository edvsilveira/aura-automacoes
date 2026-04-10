#!/usr/bin/env python3
"""
AURA - Agente de Gestão de Tarefas
Quarta, Sexta e Domingo às 7h SP (10h UTC):
envia resumo de tarefas por owner no Slack #gestao-de-tarefas.
"""

import os
import requests
from datetime import date

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_TAREFAS"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

TAREFAS_DB = "31ff915e8d4f8011986bd55487330ecd"

OWNERS = ["Eduardo", "Lucas", "Luanna", "Bruna"]


# ============================================================
# Helpers
# ============================================================

def notion_query(db_id, filter_obj=None):
    results, cursor = [], None
    while True:
        body = {"page_size": 100}
        if filter_obj: body["filter"] = filter_obj
        if cursor:     body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=NOTION_HEADERS, json=body,
        )
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"): break
        cursor = data["next_cursor"]
    return results

def get_title(page, prop):
    items = page["properties"].get(prop, {}).get("title", [])
    return "".join(t.get("plain_text", "") for t in items).strip()

def get_select(page, prop):
    s = page["properties"].get(prop, {}).get("select") or {}
    return s.get("name")

def get_status_field(page, prop):
    s = page["properties"].get(prop, {}).get("status") or {}
    return s.get("name")

def get_multi_select(page, prop):
    return [s.get("name") for s in page["properties"].get(prop, {}).get("multi_select", [])]

def get_date(page, prop):
    d = page["properties"].get(prop, {}).get("date") or {}
    return d.get("start")

def fmt_dd_mm(d_str):
    try:
        return d_str[8:10] + "/" + d_str[5:7]
    except Exception:
        return "?"

def dias_ate(prazo_str, hoje):
    try:
        prazo = date.fromisoformat(prazo_str[:10])
        return (prazo - hoje).days
    except Exception:
        return None

def secao(texto):
    return {"type": "section", "text": {"type": "mrkdwn", "text": texto}}

def titulo_owner(nome):
    return {"type": "header", "text": {"type": "plain_text", "text": nome.upper()}}

def divisor():
    return {"type": "divider"}


# ============================================================
# Main
# ============================================================

def main():
    hoje = date.today()
    print(f"AURA Gestão de Tarefas — {hoje}")

    try:
        pages = notion_query(TAREFAS_DB)
    except Exception as e:
        print(f"[ERRO] Notion: {e}")
        return

    por_owner = {o: {"atrasadas": [], "proximas": [], "no_prazo": [], "sem_prazo": []}
                 for o in OWNERS}

    total_aberto     = 0
    total_concluidas = 0
    total_atrasadas  = 0

    for p in pages:
        status = get_select(p, "Status") or get_status_field(p, "Status") or ""

        if status in ("Concluida", "Concluída", "Done", "Concluido"):
            total_concluidas += 1
            continue

        nome   = get_title(p, "Tarefa") or "(sem título)"
        owners = get_multi_select(p, "Owner")
        prazo  = get_date(p, "Data limite de entrega")

        if not owners:
            owners = ["Eduardo"]

        for owner in owners:
            if owner not in por_owner:
                continue

            total_aberto += 1

            if prazo is None:
                por_owner[owner]["sem_prazo"].append({"nome": nome, "status": status})
            else:
                dias = dias_ate(prazo, hoje)
                item = {"nome": nome, "prazo": fmt_dd_mm(prazo), "dias": dias, "status": status}
                if dias < 0:
                    por_owner[owner]["atrasadas"].append(item)
                    total_atrasadas += 1
                elif dias <= 7:
                    por_owner[owner]["proximas"].append(item)
                else:
                    por_owner[owner]["no_prazo"].append(item)

    print(f"Tarefas: {total_aberto} em aberto | {total_concluidas} concluídas | {total_atrasadas} atrasadas")

    # ---- Monta mensagem Slack ----
    dia_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"][hoje.weekday()]

    blocks = [
        # Cabeçalho principal — maior (header block nativo do Slack)
        {"type": "header", "text": {"type": "plain_text",
            "text": f"📋 CONTROLE DE TAREFAS — {dia_semana.upper()}, {hoje.strftime('%d/%m')}"}},

        # Resumo geral
        secao(
            f"• *Total em aberto:* {total_aberto}\n"
            f"• *Concluídas:* {total_concluidas}\n"
            f"• *Atrasadas:* {total_atrasadas}"
        ),
        divisor(),
    ]

    for owner in OWNERS:
        tarefas = por_owner[owner]
        total_owner = (len(tarefas["atrasadas"]) + len(tarefas["proximas"]) +
                       len(tarefas["no_prazo"]) + len(tarefas["sem_prazo"]))

        if total_owner == 0:
            continue

        # Nome do owner — bloco header (maior que tudo abaixo)
        blocks.append(titulo_owner(owner))

        # Categorias como seções separadas com texto bold (subtítulo visual)
        if tarefas["atrasadas"]:
            linhas = ["🚨 *ATRASADAS*"]
            for t in sorted(tarefas["atrasadas"], key=lambda x: x["dias"]):
                st = f" — {t['status']}" if t.get("status") else ""
                linhas.append(f"• {t['nome']} ({t['prazo']}){st}")
            blocks.append(secao("\n".join(linhas)))

        if tarefas["proximas"]:
            linhas = ["⚠️ *PRÓXIMAS DO PRAZO*"]
            for t in sorted(tarefas["proximas"], key=lambda x: x["dias"]):
                st = f" — {t['status']}" if t.get("status") else ""
                linhas.append(f"• {t['nome']} ({t['prazo']}){st}")
            blocks.append(secao("\n".join(linhas)))

        if tarefas["no_prazo"]:
            linhas = ["⏳ *DENTRO DO PRAZO*"]
            for t in sorted(tarefas["no_prazo"], key=lambda x: x["dias"]):
                st = f" — {t['status']}" if t.get("status") else ""
                linhas.append(f"• {t['nome']} ({t['prazo']}){st}")
            blocks.append(secao("\n".join(linhas)))

        if tarefas["sem_prazo"]:
            linhas = ["❓ *SEM PRAZO*"]
            for t in tarefas["sem_prazo"]:
                st = f" — {t['status']}" if t.get("status") else ""
                linhas.append(f"• {t['nome']}{st}")
            blocks.append(secao("\n".join(linhas)))

        blocks.append(divisor())

    if len(blocks) > 50:
        blocks = blocks[:50]

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
        print(f"Slack: {'ok' if resp.text == 'ok' else resp.text}")
    except Exception as e:
        print(f"[ERRO] Slack: {e}")

    print("=== Gestão de Tarefas finalizado ===")


if __name__ == "__main__":
    main()
