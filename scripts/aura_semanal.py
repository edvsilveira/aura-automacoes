#!/usr/bin/env python3
"""
AURA - Agente Semanal
Toda sexta-feira: busca dados do Notion, cria página de análise, notifica Slack.
"""

import os
import json
import requests
import calendar
from datetime import datetime, date, timedelta
import anthropic

# --- Credenciais ---
NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK"]
client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# --- IDs ---
BALANCO_DB      = "238f915e8d4f80a7ae63e166f6f3e50f"
METAS_DB        = "248f915e8d4f8003bcc7e513490b0603"
RENOVACOES_DB   = "245f915e8d4f803e9a7cc220338cb568"
CRM_LEADS_DB    = "2e7f915e8d4f807e8fb4fa615f02bd15"
ERP_PARENT_PAGE = "322f915e8d4f80229ec1d2748e174d1a"

MESES_PT = {
    1: "JANEIRO", 2: "FEVEREIRO", 3: "MARÇO", 4: "ABRIL",
    5: "MAIO", 6: "JUNHO", 7: "JULHO", 8: "AGOSTO",
    9: "SETEMBRO", 10: "OUTUBRO", 11: "NOVEMBRO", 12: "DEZEMBRO",
}

OWNER_COLORS = {"Lucas": "blue", "AURA": "yellow", "Luanna": "pink"}


def numero_semana(day):
    if day <= 7:  return 1
    if day <= 14: return 2
    if day <= 21: return 3
    if day <= 28: return 4
    return 5


# ============================================================
# Helpers Notion
# ============================================================

def notion_query(db_id, filter_obj=None, sorts=None):
    """Query all pages from a database, handling pagination."""
    results, cursor = [], None
    while True:
        body = {"page_size": 100}
        if filter_obj: body["filter"] = filter_obj
        if sorts:      body["sorts"]  = sorts
        if cursor:     body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=NOTION_HEADERS, json=body
        )
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"): break
        cursor = data["next_cursor"]
    return results


def notion_create_page(parent_id, title):
    """Create an empty page and return its id + url."""
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={
            "parent": {"page_id": parent_id},
            "icon": {"type": "emoji", "emoji": "📊"},
            "properties": {"title": {"title": [{"text": {"content": title}}]}},
            "children": [],
        },
    )
    r.raise_for_status()
    return r.json()


def notion_append_blocks(block_id, children):
    """Append blocks in batches of 50 (Notion limit per call)."""
    for i in range(0, len(children), 50):
        batch = children[i:i+50]
        r = requests.patch(
            f"https://api.notion.com/v1/blocks/{block_id}/children",
            headers=NOTION_HEADERS,
            json={"children": batch},
        )
        r.raise_for_status()


# ============================================================
# Block builders
# ============================================================

def rt(text, bold=False, color=None):
    ann = {
        "bold": bold, "italic": False, "strikethrough": False,
        "underline": False, "code": False, "color": color or "default",
    }
    return {"type": "text", "text": {"content": text}, "annotations": ann}


def callout_block(emoji, rich_text):
    return {
        "object": "block", "type": "callout",
        "callout": {
            "icon": {"type": "emoji", "emoji": emoji},
            "rich_text": rich_text,
            "color": "gray_background",
        },
    }


def h2_block(text):
    return {
        "object": "block", "type": "heading_2",
        "heading_2": {"rich_text": [rt(text)]},
    }


def para_block(rich_text):
    return {
        "object": "block", "type": "paragraph",
        "paragraph": {"rich_text": rich_text},
    }


def bullet_block(rich_text):
    return {
        "object": "block", "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": rich_text},
    }


def divider_block():
    return {"object": "block", "type": "divider", "divider": {}}


def fmt_brl(v):
    try:
        return f"R${float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$0,00"


def pct(valor, meta):
    try:
        return round(float(valor) / float(meta) * 100, 1) if float(meta) > 0 else 0.0
    except Exception:
        return 0.0


# ============================================================
# Serialização compacta para prompt
# ============================================================

def compact(pages, max_chars=6000):
    """Return a compact JSON string of Notion pages (properties only)."""
    out = []
    for p in pages:
        props = {}
        for k, v in p.get("properties", {}).items():
            ptype = v.get("type", "")
            if ptype == "title":
                props[k] = "".join(t.get("plain_text", "") for t in v.get("title", []))
            elif ptype == "rich_text":
                props[k] = "".join(t.get("plain_text", "") for t in v.get("rich_text", []))
            elif ptype == "date":
                d = v.get("date") or {}
                props[k] = d.get("start")
            elif ptype == "number":
                props[k] = v.get("number")
            elif ptype == "select":
                s = v.get("select") or {}
                props[k] = s.get("name")
            elif ptype == "multi_select":
                props[k] = [s.get("name") for s in v.get("multi_select", [])]
            elif ptype == "formula":
                f = v.get("formula") or {}
                props[k] = f.get("number") or f.get("string")
            elif ptype == "checkbox":
                props[k] = v.get("checkbox")
            elif ptype == "url":
                props[k] = v.get("url")
        out.append(props)
    text = json.dumps(out, ensure_ascii=False)
    return text[:max_chars] + ("..." if len(text) > max_chars else "")


# ============================================================
# Main
# ============================================================

def main():
    today         = date.today()
    inicio_semana = today - timedelta(days=4)
    mes_inicio    = date(today.year, today.month, 1)
    mes_fim       = date(today.year, today.month,
                         calendar.monthrange(today.year, today.month)[1])
    mes_nome      = MESES_PT[today.month]
    num_semana    = numero_semana(today.day)
    titulo        = f"AURA - {mes_nome} - SEMANA {num_semana}"

    print(f"AURA Agente Semanal — {today}")
    print(f"Título: {titulo} | Semana: {inicio_semana} → {today} | Mês: {mes_inicio} → {mes_fim}")

    # ---- 1. Metas ----
    print("\n[1/4] Buscando metas...")
    try:
        metas_raw = notion_query(METAS_DB)
        metas_compact = compact(metas_raw, 3000)
    except Exception as e:
        print(f"  [ERRO] Metas: {e}")
        metas_compact = "[]"

    # ---- 2. Balanço Geral ----
    print("[2/4] Buscando balanço geral...")
    try:
        balanco_raw = notion_query(BALANCO_DB)
        balanco_compact = compact(balanco_raw, 8000)
    except Exception as e:
        print(f"  [ERRO] Balanço: {e}")
        balanco_compact = "[]"

    # ---- 3. CRM Leads (mês inteiro) ----
    print("[3/4] Buscando leads do CRM...")
    try:
        leads_raw = notion_query(CRM_LEADS_DB, filter_obj={
            "and": [
                {"property": "Primeiro contato", "date": {"on_or_after":  str(mes_inicio)}},
                {"property": "Primeiro contato", "date": {"on_or_before": str(mes_fim)}},
            ]
        })
        leads_compact = compact(leads_raw, 8000)
    except Exception as e:
        print(f"  [ERRO] CRM Leads: {e}")
        leads_compact = "[]"

    # ---- 4. Renovações ----
    print("[4/4] Buscando renovações...")
    try:
        renovacoes_raw = notion_query(RENOVACOES_DB)
        renovacoes_compact = compact(renovacoes_raw, 8000)
    except Exception as e:
        print(f"  [ERRO] Renovações: {e}")
        renovacoes_compact = "[]"

    # ---- 5. Análise via Claude ----
    print("\nAnalisando dados com IA...")

    analysis_prompt = f"""Você é o analista de dados da AURA Consultoria.
Analise os dados abaixo e retorne SOMENTE um JSON puro (sem markdown, sem explicações).

DATAS:
- hoje: {today}
- inicio_semana (seg): {inicio_semana}
- mes_inicio: {mes_inicio}
- mes_fim: {mes_fim}
- mes_nome: {mes_nome}
- numero_semana: {num_semana}

METAS DE FATURAMENTO (Notion — identifique mês={mes_nome}, extraia Lucas/AURA/Luanna):
{metas_compact}

BALANÇO GERAL (todas as transações — filtre por data para semana e mês):
{balanco_compact}

CRM DE LEADS (leads do mês — "Nutricionista" identifica responsável, "Primeiro contato" é a data de entrada):
{leads_compact}

RENOVAÇÕES (base única — "Consultoria"=Premium→Lucas ou Luanna / Comfort→AURA, "Data" para filtrar):
{renovacoes_compact}

REGRAS:
- leads_frios_semana = leads da semana onde "Onde parou" = "Apenas clicou no link"
- leads_semana = "Primeiro contato" entre {inicio_semana} e {today}
- vendas_semana = transações financeiras com data entre {inicio_semana} e {today}
- renovacoes_semana = renovações com "Data" entre {inicio_semana} e {today}
- pipeline_ativo = renovações com "Data" entre {today} e {mes_fim} E status Em andamento OU Mensagem enviada
- taxa_renovacao = renovaram_mes / total_renovacoes_mes × 100 (renovaram = Renovação + Renovação Antecipada)
- Se não encontrar metas para o mês, use: lucas=25000, aura=20000, luanna=10000

Retorne EXATAMENTE este JSON (sem texto fora dele):
{{
  "metas": {{"lucas": 0, "aura": 0, "luanna": 0, "total": 0}},
  "financeiro": {{
    "faturamento_semana": 0,
    "faturamento_mes_lucas": 0,
    "faturamento_mes_aura": 0,
    "faturamento_mes_luanna": 0,
    "faturamento_mes_total": 0,
    "vendas_semana": [
      {{"nome": "", "valor": 0, "responsavel": "", "produto": "", "forma_pag": "", "data_dd_mm": ""}}
    ]
  }},
  "leads": {{
    "total_semana": 0,
    "conversoes_semana": 0,
    "leads_frios_semana": 0,
    "taxa_conversao_semana": 0.0,
    "total_mes": 0,
    "vendas_mes": 0,
    "taxa_conversao_mes": 0.0,
    "pipeline_ativo": 0,
    "leads_semana": [
      {{"nome": "", "data_dd_mm": "", "nutricionista": "", "origem": "", "status": ""}}
    ],
    "funil": {{
      "Venda": 0, "Qualificação": 0, "Proposta feita": 0, "Novo": 0,
      "Desistiu": 0, "Negociação": 0, "Link enviado": 0, "FUP": 0
    }}
  }},
  "renovacoes": {{
    "previstas_semana": 0,
    "confirmadas_semana": 0,
    "perdidas_semana": 0,
    "renovaram_mes": 0,
    "antecipadas_mes": 0,
    "confirmadas_mes": 0,
    "perdidas_mes": 0,
    "em_aberto_mes": 0,
    "total_mes": 0,
    "taxa_renovacao": 0.0,
    "renovacoes_semana": [
      {{"nome": "", "data_dd_mm": "", "responsavel": "", "consultoria": "", "status": ""}}
    ],
    "pipeline_ativo": [
      {{"nome": "", "data_dd_mm": "", "responsavel": "", "consultoria": "", "status": ""}}
    ]
  }},
  "analises": {{
    "leitura_faturamento": "",
    "leitura_leads": "",
    "leitura_renovacoes": "",
    "leitura_funil": "",
    "conclusao_p1": "",
    "conclusao_p2": "",
    "conclusao_p3": "",
    "proximos_passos": ["", "", ""]
  }}
}}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": analysis_prompt}],
        )
        raw = response.content[0].text.strip()
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
        data = json.loads(raw)
        print("  Análise concluída.")
    except Exception as e:
        print(f"  [ERRO] Análise IA: {e}")
        return

    m  = data["metas"]
    f  = data["financeiro"]
    l  = data["leads"]
    r  = data["renovacoes"]
    a  = data["analises"]

    ini     = inicio_semana.strftime("%d/%m")
    fim_sem = today.strftime("%d/%m")
    fim_mes = mes_fim.strftime("%d/%m")

    pct_lucas  = pct(f["faturamento_mes_lucas"],  m["lucas"])
    pct_aura   = pct(f["faturamento_mes_aura"],   m["aura"])
    pct_luanna = pct(f["faturamento_mes_luanna"],  m["luanna"])
    pct_total  = pct(f["faturamento_mes_total"],  m["total"])
    status_meta = "✅" if pct_total >= 90 else ("⚠️" if pct_total >= 60 else "🔴")

    # ---- Construção dos blocos Notion ----
    blocks = []

    # === FATURAMENTO ===
    blocks.append(callout_block("💰", [
        rt(f"Faturamento Mensal Acumulado — {mes_nome} {today.year}\n", bold=True),
        rt("Meta do mês: "), rt(fmt_brl(m["total"]), bold=True),
        rt(" · Total acumulado: "), rt(fmt_brl(f["faturamento_mes_total"]), bold=True, color="green"),
        rt(f" — {pct_total}% da meta {status_meta}", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("Lucas (Premium): ", bold=True),
        rt(fmt_brl(f["faturamento_mes_lucas"]), color="blue"),
        rt(f" / {fmt_brl(m['lucas'])} → "),
        rt(f"{pct_lucas}%", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("AURA (Comfort): ", bold=True),
        rt(fmt_brl(f["faturamento_mes_aura"]), color="yellow"),
        rt(f" / {fmt_brl(m['aura'])} → "),
        rt(f"{pct_aura}%", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("Luanna (Premium): ", bold=True),
        rt(fmt_brl(f["faturamento_mes_luanna"]), color="pink"),
        rt(f" / {fmt_brl(m['luanna'])} → "),
        rt(f"{pct_luanna}%", bold=True),
    ]))
    blocks.append(para_block([rt("Leitura: "), rt(a["leitura_faturamento"])]))
    blocks.append(divider_block())

    # === VENDAS DA SEMANA ===
    blocks.append(callout_block("📋", [
        rt(f"Vendas da Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{len(f['vendas_semana'])} venda(s) registrada(s) · Faturamento bruto: "),
        rt(fmt_brl(f["faturamento_semana"]), bold=True, color="green"),
    ]))
    if f["vendas_semana"]:
        for v in f["vendas_semana"]:
            cor = OWNER_COLORS.get(v.get("responsavel", ""), "gray")
            blocks.append(bullet_block([
                rt(fmt_brl(v.get("valor", 0)), bold=True, color="green"),
                rt(f" ({v.get('forma_pag', '')}) — {v.get('nome', '')} — responsável: "),
                rt(v.get("responsavel", ""), bold=True, color=cor),
                rt(f" · {v.get('produto', '')} · {v.get('data_dd_mm', '')}"),
            ]))
    else:
        blocks.append(para_block([rt("Nenhuma venda registrada no período.")]))
    blocks.append(divider_block())

    # === CRM DE LEADS ===
    blocks.append(callout_block("🎯", [
        rt(f"CRM de Leads — Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{l['total_semana']} novo(s) lead(s) na semana · {l['conversoes_semana']} conversão(ões) imediata(s)"),
    ]))
    if l["leads_semana"]:
        for lead in l["leads_semana"]:
            cor = OWNER_COLORS.get(lead.get("nutricionista", ""), "gray")
            status_cor = ("green" if lead.get("status") == "Venda"
                          else "red" if lead.get("status") == "Desistiu" else None)
            items = [
                rt(f"{lead.get('nome', '')} — {lead.get('data_dd_mm', '')} — "),
                rt(lead.get("nutricionista", ""), bold=True, color=cor),
                rt(f" — {lead.get('origem', '')} → "),
                rt(lead.get("status", ""), bold=True, color=status_cor) if status_cor
                else rt(lead.get("status", "")),
            ]
            blocks.append(bullet_block(items))
    else:
        blocks.append(para_block([rt("Nenhum novo lead registrado no período.")]))
    blocks.append(para_block([rt("Leitura: "), rt(a["leitura_leads"])]))
    blocks.append(divider_block())

    # === RENOVAÇÕES ===
    blocks.append(callout_block("🔄", [
        rt(f"Renovações — Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{r['previstas_semana']} renovação(ões) prevista(s) na semana · "),
        rt(f"{r['confirmadas_semana']} confirmada(s) · {r['perdidas_semana']} não renovou(aram)"),
    ]))
    if r["renovacoes_semana"]:
        for ren in r["renovacoes_semana"]:
            cor = OWNER_COLORS.get(ren.get("responsavel", ""), "gray")
            status_ren = ren.get("status", "")
            status_cor = ("green" if "Renovação" in status_ren
                          else "red" if "Não renovou" in status_ren else None)
            blocks.append(bullet_block([
                rt(f"{ren.get('nome', '')} — {ren.get('data_dd_mm', '')} — "),
                rt(ren.get("responsavel", ""), bold=True, color=cor),
                rt(f" — {ren.get('consultoria', '')} → "),
                rt(status_ren, bold=True, color=status_cor) if status_cor else rt(status_ren),
            ]))
    else:
        blocks.append(para_block([rt("Nenhuma renovação prevista para esta semana.")]))

    blocks.append(h2_block(f"Pipeline Ativo — Vencendo até {fim_mes}"))
    if r["pipeline_ativo"]:
        for p_item in r["pipeline_ativo"]:
            cor = OWNER_COLORS.get(p_item.get("responsavel", ""), "gray")
            blocks.append(bullet_block([
                rt(f"{p_item.get('nome', '')} — {p_item.get('data_dd_mm', '')} — "),
                rt(p_item.get("responsavel", ""), bold=True, color=cor),
                rt(f" — {p_item.get('consultoria', '')} → {p_item.get('status', '')}"),
            ]))
    else:
        blocks.append(para_block([rt("Nenhuma renovação em aberto até o fim do mês.")]))
    blocks.append(para_block([rt("Leitura: "), rt(a["leitura_renovacoes"])]))
    blocks.append(divider_block())

    # === FUNIL DO MÊS ===
    funil = l["funil"]
    blocks.append(callout_block("📊", [
        rt(f"Funil do Mês — {mes_nome} {today.year}\n", bold=True),
        rt(f"{l['total_mes']} leads no mês · Taxa de conversão: "),
        rt(f"{l['taxa_conversao_mes']}%", bold=True),
        rt(f" ({funil.get('Venda', 0)}/{l['total_mes']}) · Pipeline ativo: "),
        rt(f"{l['pipeline_ativo']} leads", bold=True),
    ]))
    blocks.append(bullet_block([rt("✅ Venda: ", bold=True, color="green"), rt(str(funil.get("Venda", 0)), bold=True, color="green")]))
    blocks.append(bullet_block([rt(f"Qualificação: {funil.get('Qualificação', 0)}")]))
    blocks.append(bullet_block([rt(f"Proposta feita: {funil.get('Proposta feita', 0)}")]))
    blocks.append(bullet_block([rt(f"Novo: {funil.get('Novo', 0)}")]))
    blocks.append(bullet_block([rt("Desistiu: ", bold=True, color="red"), rt(str(funil.get("Desistiu", 0)), bold=True, color="red")]))
    blocks.append(bullet_block([rt(f"Negociação: {funil.get('Negociação', 0)}")]))
    blocks.append(bullet_block([rt(f"Link enviado: {funil.get('Link enviado', 0)}")]))
    blocks.append(bullet_block([rt(f"FUP (follow-up): {funil.get('FUP', 0)}")]))
    blocks.append(para_block([rt("Leitura: "), rt(a["leitura_funil"])]))
    blocks.append(divider_block())

    # === CONCLUSÃO EXECUTIVA ===
    blocks.append(h2_block("Conclusão Executiva"))
    blocks.append(para_block([rt(a["conclusao_p1"])]))
    blocks.append(para_block([rt(a["conclusao_p2"])]))
    blocks.append(para_block([rt(a["conclusao_p3"])]))
    blocks.append(para_block([rt("")]))
    blocks.append(para_block([rt("Próximos passos:", bold=True)]))
    for passo in a.get("proximos_passos", []):
        if passo:
            blocks.append(bullet_block([rt(passo)]))

    # ---- Criar página e anexar blocos ----
    print("\nCriando página no Notion...")
    page_url = ""
    try:
        page = notion_create_page(ERP_PARENT_PAGE, titulo)
        page_id  = page["id"]
        page_url = page.get("url", "")
        notion_append_blocks(page_id, blocks)
        print(f"  Página criada: {page_url}")
    except Exception as e:
        print(f"  [ERRO] Página Notion: {e}")

    # ---- Slack ----
    print("Enviando notificação Slack...")
    slack_payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": f"📊 {titulo}"}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"📅 *SEMANA ({ini}–{fim_sem})*"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f"• *Faturamento:* {fmt_brl(f['faturamento_semana'])}\n"
                f"• *Novos leads:* {l['total_semana']}  |  Vendas: {l['conversoes_semana']}  |  Conversão: {l['taxa_conversao_semana']}%\n"
                f"   ↳ Leads frios (apenas clicaram no link): {l['leads_frios_semana']}\n"
                f"• *Renovações:* {r['previstas_semana']} prevista(s) na semana\n"
                f"   ↳ Previstas: {r['previstas_semana']}  |  Confirmadas: {r['confirmadas_semana']}  |  Perdidas: {r['perdidas_semana']}"
            )}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"📆 *{mes_nome} — ACUMULADO*"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f"• *Faturamento:* {fmt_brl(f['faturamento_mes_total'])} — {pct_total}% da meta\n"
                f"   ↳ Lucas: {pct_lucas}%  |  AURA: {pct_aura}%  |  Luanna: {pct_luanna}%\n"
                f"• *Leads no mês:* {l['total_mes']}  |  Vendas: {l['vendas_mes']}  |  Conversão: {l['taxa_conversao_mes']}%\n"
                f"• *Renovações:* {r['renovaram_mes']}/{r['total_mes']} — {r['taxa_renovacao']}% de taxa de renovação\n"
                f"   ↳ Antecipadas: {r['antecipadas_mes']}  |  Confirmadas: {r['confirmadas_mes']}  |  Perdidas: {r['perdidas_mes']}  |  Em aberto: {r['em_aberto_mes']}"
            )}},
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f"<{page_url}|Ver relatório no Notion>" if page_url else "Relatório criado no Notion."
            )}},
        ]
    }
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=slack_payload)
        print(f"  Slack: {'ok' if resp.text == 'ok' else resp.text}")
    except Exception as e:
        print(f"  [ERRO] Slack: {e}")

    print(f"\n=== Agente Semanal finalizado: {titulo} ===")


if __name__ == "__main__":
    main()
