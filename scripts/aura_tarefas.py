#!/usr/bin/env python3
"""
AURA - Agente de Gestão de Tarefas
Quarta, Sexta e Domingo às 7h SP (10h UTC):
envia resumo de tarefas por owner no Slack #gestao-de-tarefas.
"""

import os
import requests
from datetime import date, timedelta

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_TAREFAS"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

TAREFAS_DB = "31ff915e8d4f8011986bd55487330ecd"

OWNERS = ["Eduardo", "Lucas", "Luanna", "Bruna"]
SLACK_MENTIONS = {
    "Eduardo": "@Edu",
    "Lucas":   "@Lucas Mendonca",
    "Luanna":  "@Luanna Estebanez",
    "Bruna":   "@Bruna Mendonca",
}


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


# ============================================================
# Main
# ============================================================

def main():
    hoje = date.today()
    print(f"AURA Gestão de Tarefas — {hoje}")

    # Busca todas as tarefas não concluídas
    try:
        pages = notion_query(TAREFAS_DB)
    except Exception as e:
        print(f"[ERRO] Notion: {e}")
        return

    # Agrupa por owner
    por_owner = {o: {"atrasadas": [], "proximas": [], "no_prazo": [], "sem_prazo": []}
                 for o in OWNERS}

    total_atrasadas = 0
    total_proximas  = 0
    total_no_prazo  = 0
    total_sem_prazo = 0

    for p in pages:
        status = get_select(p, "Status") or ""
        if status == "Concluida":
            continue

        nome   = get_title(p, "Tarefa") or "(sem título)"
        owners = get_multi_select(p, "Owner")
        prazo  = get_date(p, "Data limite de entrega")

        if not owners:
            owners = ["Eduardo"]  # fallback

        for owner in owners:
            if owner not in por_owner:
                continue

            if prazo is None:
                por_owner[owner]["sem_prazo"].append({"nome": nome, "prazo": None})
                total_sem_prazo += 1
            else:
                dias = dias_ate(prazo, hoje)
                item = {"nome": nome, "prazo": fmt_dd_mm(prazo), "dias": dias}
                if dias < 0:
                    por_owner[owner]["atrasadas"].append(item)
                    total_atrasadas += 1
                elif dias <= 7:
                    por_owner[owner]["proximas"].append(item)
                    total_proximas += 1
                else:
                    por_owner[owner]["no_prazo"].append(item)
                    total_no_prazo += 1

    total_geral = total_atrasadas + total_proximas + total_no_prazo + total_sem_prazo
    print(f"Tarefas: {total_geral} total | {total_atrasadas} atrasadas | "
          f"{total_proximas} próximas | {total_no_prazo} no prazo | {total_sem_prazo} sem prazo")

    # ---- Monta mensagem Slack ----
    dia_semana = ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"][hoje.weekday() % 7]
    # weekday(): 0=Mon, 6=Sun → ajuste para PT
    dia_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"][hoje.weekday()]

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"📋 CONTROLE DE TAREFAS — {dia_semana} {hoje.strftime('%d/%m')}"}},
        {"type": "section", "text": {"type": "mrkdwn",
            "text": (f"*Total: {total_geral} tarefa(s) em aberto* — "
                     f"⚠️ {total_atrasadas} atrasada(s) · "
                     f"🔔 {total_proximas} próxima(s) do prazo · "
                     f"✅ {total_no_prazo} dentro do prazo"
                     + (f" · ❓ {total_sem_prazo} sem prazo" if total_sem_prazo > 0 else ""))}},
        {"type": "divider"},
    ]

    for owner in OWNERS:
        tarefas = por_owner[owner]
        total_owner = (len(tarefas["atrasadas"]) + len(tarefas["proximas"]) +
                       len(tarefas["no_prazo"]) + len(tarefas["sem_prazo"]))

        if total_owner == 0:
            continue

        mention = SLACK_MENTIONS.get(owner, f"@{owner}")
        linhas = [f"*{mention}* — {total_owner} tarefa(s)"]

        if tarefas["atrasadas"]:
            linhas.append(f"\n⚠️ *ATRASADAS ({len(tarefas['atrasadas'])})*")
            for t in sorted(tarefas["atrasadas"], key=lambda x: x["dias"]):
                linhas.append(f"   • {t['nome']} — venceu {t['prazo']}")

        if tarefas["proximas"]:
            linhas.append(f"\n🔔 *PRÓXIMAS DO PRAZO ({len(tarefas['proximas'])})*")
            for t in sorted(tarefas["proximas"], key=lambda x: x["dias"]):
                linhas.append(f"   • {t['nome']} — vence {t['prazo']}")

        if tarefas["no_prazo"]:
            linhas.append(f"\n✅ *DENTRO DO PRAZO ({len(tarefas['no_prazo'])})*")
            for t in sorted(tarefas["no_prazo"], key=lambda x: x["dias"]):
                linhas.append(f"   • {t['nome']} — vence {t['prazo']}")

        if tarefas["sem_prazo"]:
            linhas.append(f"\n❓ *SEM PRAZO ({len(tarefas['sem_prazo'])})*")
            for t in tarefas["sem_prazo"]:
                linhas.append(f"   • {t['nome']}")

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(linhas)}})
        blocks.append({"type": "divider"})

    # Slack tem limite de 50 blocos por mensagem
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
