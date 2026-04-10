#!/usr/bin/env python3
"""
AURA - Agente Semanal (sem dependência de IA)
Toda sexta-feira: busca dados do Notion, cria página de análise, notifica Slack.
"""

import os
import requests
import calendar
from datetime import date, timedelta

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

BALANCO_DB      = "238f915e8d4f80a7ae63e166f6f3e50f"
METAS_DB        = "248f915e8d4f8003bcc7e513490b0603"
RENOVACOES_DB   = "245f915e8d4f803e9a7cc220338cb568"
CRM_LEADS_DB    = "2e7f915e8d4f807e8fb4fa615f02bd15"
ERP_PARENT_PAGE = "322f915e8d4f80229ec1d2748e174d1a"

MESES_PT = {
    1:"JANEIRO",2:"FEVEREIRO",3:"MARÇO",4:"ABRIL",5:"MAIO",6:"JUNHO",
    7:"JULHO",8:"AGOSTO",9:"SETEMBRO",10:"OUTUBRO",11:"NOVEMBRO",12:"DEZEMBRO",
}
OWNER_COLORS = {"Lucas":"blue","AURA":"yellow","Luanna":"pink"}


# ============================================================
# Helpers de data e número
# ============================================================

def numero_semana(day):
    if day <= 7:  return 1
    if day <= 14: return 2
    if day <= 21: return 3
    if day <= 28: return 4
    return 5

def fmt_brl(v):
    try:
        s = f"{float(v):,.2f}"
        s = s.replace(",","X").replace(".",",").replace("X",".")
        return f"R${s}"
    except Exception:
        return "R$0,00"

def pct(valor, meta):
    try:
        return round(float(valor) / float(meta) * 100, 1) if float(meta) > 0 else 0.0
    except Exception:
        return 0.0

def fmt_dd_mm(d_str):
    try:
        return d_str[8:10] + "/" + d_str[5:7]
    except Exception:
        return ""

def in_range(d_str, start, end):
    try:
        d = d_str[:10]
        return str(start) <= d <= str(end)
    except Exception:
        return False


# ============================================================
# Extratores de propriedades Notion
# ============================================================

def get_title(page, prop):
    items = page["properties"].get(prop, {}).get("title", [])
    return "".join(t.get("plain_text","") for t in items)

def get_text(page, prop):
    items = page["properties"].get(prop, {}).get("rich_text", [])
    return "".join(t.get("plain_text","") for t in items)

def get_select(page, prop):
    s = page["properties"].get(prop, {}).get("select") or {}
    return s.get("name")

def get_status(page, prop):
    s = page["properties"].get(prop, {}).get("status") or {}
    return s.get("name")

def get_number(page, prop):
    return page["properties"].get(prop, {}).get("number")

def get_date(page, prop):
    d = page["properties"].get(prop, {}).get("date") or {}
    return d.get("start")


# ============================================================
# Helpers Notion API
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

def notion_create_page(parent_id, title):
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={
            "parent": {"page_id": parent_id},
            "icon": {"type":"emoji","emoji":"📊"},
            "properties": {"title": {"title": [{"text":{"content":title}}]}},
            "children": [],
        },
    )
    r.raise_for_status()
    return r.json()

def notion_append(block_id, children):
    for i in range(0, len(children), 50):
        r = requests.patch(
            f"https://api.notion.com/v1/blocks/{block_id}/children",
            headers=NOTION_HEADERS,
            json={"children": children[i:i+50]},
        )
        r.raise_for_status()


# ============================================================
# Construtores de blocos Notion
# ============================================================

def rt(text, bold=False, color=None):
    ann = {"bold":bold,"italic":False,"strikethrough":False,
           "underline":False,"code":False,"color":color or "default"}
    return {"type":"text","text":{"content":text},"annotations":ann}

def callout(emoji, rich_text):
    return {"object":"block","type":"callout","callout":{
        "icon":{"type":"emoji","emoji":emoji},
        "rich_text":rich_text,"color":"gray_background"}}

def h2(text):
    return {"object":"block","type":"heading_2",
            "heading_2":{"rich_text":[rt(text)]}}

def para(rich_text):
    return {"object":"block","type":"paragraph",
            "paragraph":{"rich_text":rich_text}}

def bullet(rich_text):
    return {"object":"block","type":"bulleted_list_item",
            "bulleted_list_item":{"rich_text":rich_text}}

def divider():
    return {"object":"block","type":"divider","divider":{}}


# ============================================================
# Geração de textos de análise
# ============================================================

def leitura_faturamento(fat_total, meta_total, fat_lucas, meta_lucas,
                         fat_aura, meta_aura, fat_luanna, meta_luanna):
    pct_t = pct(fat_total, meta_total)
    restante = meta_total - fat_total
    lideres = sorted(
        [("Lucas", pct(fat_lucas, meta_lucas)),
         ("AURA",  pct(fat_aura,  meta_aura)),
         ("Luanna",pct(fat_luanna,meta_luanna))],
        key=lambda x: x[1], reverse=True
    )
    t = f"Acumulado de {fmt_brl(fat_total)} ({pct_t}% da meta). "
    t += f"{lideres[0][0]} lidera com {lideres[0][1]}% da meta individual"
    if lideres[1][1] > 0:
        t += f", seguido de {lideres[1][0]} com {lideres[1][1]}%"
    t += ". "
    if restante > 0:
        t += f"Faltam {fmt_brl(restante)} para bater a meta total do mês."
    else:
        t += "Meta total do mês atingida!"
    return t

def leitura_leads(total_semana, conversoes, frios, taxa_semana, total_mes, taxa_mes):
    t = f"{total_semana} lead(s) na semana"
    if total_semana > 0:
        t += f", {conversoes} conversão(ões) imediata(s) (taxa de {taxa_semana}%)."
    else:
        t += " — nenhum novo lead registrado."
    if frios > 0:
        t += f" {frios} lead(s) frio(s) (apenas clicaram no link)."
    t += f" No mês: {total_mes} leads com taxa de conversão de {taxa_mes}%."
    return t

def leitura_renovacoes(renovaram, perdidas, em_aberto, total_mes, taxa):
    t = f"Taxa de renovação do mês: {taxa}% ({renovaram}/{total_mes}). "
    if perdidas > 0:
        t += f"{perdidas} não renovou(aram) no período. "
    if em_aberto > 0:
        t += f"{em_aberto} renovação(ões) ainda em aberto até o fim do mês — acompanhar."
    else:
        t += "Nenhuma renovação pendente em aberto."
    return t

def leitura_funil(total, vendas, pipeline, funil):
    taxa = pct(vendas, total)
    maior = max(funil.items(), key=lambda x: x[1]) if funil else ("—", 0)
    t = f"{total} leads no mês, taxa de conversão de {taxa}%. "
    t += f"Pipeline ativo com {pipeline} leads. "
    if maior[1] > 0:
        t += f"Maior concentração em '{maior[0]}' ({maior[1]} leads)."
    return t

def conclusao(fat_total, meta_total, total_leads_semana, conversoes_semana,
               renovaram, total_ren, em_aberto):
    pct_t = pct(fat_total, meta_total)
    p1 = (f"O faturamento acumulado está em {fmt_brl(fat_total)} ({pct_t}% da meta). "
          + ("Bom ritmo — manter foco nas conversões para fechar o mês acima da meta."
             if pct_t >= 60 else "Ritmo abaixo do esperado — avaliar ações para acelerar vendas."))
    p2 = (f"Na semana foram registrados {total_leads_semana} lead(s) com {conversoes_semana} "
          f"conversão(ões). Acompanhar os leads quentes do pipeline para maximizar conversões.")
    taxa_ren = pct(renovaram, total_ren)
    p3 = (f"Taxa de renovação do mês em {taxa_ren}% ({renovaram}/{total_ren}). "
          + (f"{em_aberto} renovação(ões) ainda em aberto — priorizar contato para fechar o mês."
             if em_aberto > 0 else "Todas as renovações do mês resolvidas."))
    return p1, p2, p3


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
    print(f"Título: {titulo} | Semana: {inicio_semana}→{today} | Mês: {mes_inicio}→{mes_fim}")

    # ---- 1. Metas ----
    print("\n[1/4] Buscando metas...")
    metas = {"Lucas": 25000, "AURA": 20000, "Luanna": 10000}
    try:
        for p in notion_query(METAS_DB):
            nutri = get_title(p, "Nutri")
            meta  = get_number(p, "Meta") or 0
            if nutri in metas:
                metas[nutri] = meta
        print(f"  Metas: {metas}")
    except Exception as e:
        print(f"  [ERRO] Metas: {e} — usando valores padrão")
    meta_total = sum(metas.values())

    # ---- 2. Balanço Geral (mês) ----
    print("[2/4] Buscando balanço geral...")
    fat_mes   = {"Lucas":0.0,"AURA":0.0,"Luanna":0.0}
    vendas_semana_list = []
    despesas_semana_list = []
    fat_semana = 0.0
    try:
        balanco = notion_query(BALANCO_DB, filter_obj={"and":[
            {"property":"Data da transação","date":{"on_or_after": str(mes_inicio)}},
            {"property":"Data da transação","date":{"on_or_before":str(mes_fim)}},
        ]})
        for p in balanco:
            valor    = get_number(p, "Valor ") or 0  # propriedade tem espaço no final
            carater  = get_select(p, "Caráter") or ""
            resp     = get_select(p, "Responsável") or ""
            data_tx  = get_date(p, "Data da transação") or ""
            nome_tx  = get_title(p, "Nome")
            origem   = get_text(p, "Origem/Destino")
            modo_pag = get_select(p, "Modo de Pagto") or ""

            if carater != "Despesa" and valor > 0:
                if resp in fat_mes:
                    fat_mes[resp] += valor
                if in_range(data_tx, inicio_semana, today):
                    fat_semana += valor
                    vendas_semana_list.append({
                        "nome": origem or nome_tx,
                        "valor": valor,
                        "responsavel": resp,
                        "produto": nome_tx,
                        "forma_pag": modo_pag,
                        "data_dd_mm": fmt_dd_mm(data_tx),
                        "carater": carater,
                    })
            elif carater == "Despesa" and in_range(data_tx, inicio_semana, today):
                despesas_semana_list.append({
                    "nome": origem or nome_tx,
                    "valor": abs(valor),
                    "forma_pag": modo_pag,
                    "data_dd_mm": fmt_dd_mm(data_tx),
                })
        fat_total_mes = sum(fat_mes.values())
        print(f"  Faturamento mês: {fmt_brl(fat_total_mes)} | Semana: {fmt_brl(fat_semana)}")
    except Exception as e:
        print(f"  [ERRO] Balanço: {e}")
        fat_total_mes = 0.0

    # ---- 3. CRM Leads (mês) ----
    print("[3/4] Buscando leads do CRM...")
    leads_semana_list  = []
    funil              = {}
    total_leads_mes    = 0
    vendas_mes         = 0
    pipeline_ativo     = 0
    leads_frios_semana = 0
    conversoes_semana  = 0
    try:
        leads = notion_query(CRM_LEADS_DB, filter_obj={"and":[
            {"property":"Primeiro contato","date":{"on_or_after": str(mes_inicio)}},
            {"property":"Primeiro contato","date":{"on_or_before":str(mes_fim)}},
        ]})
        total_leads_mes = len(leads)
        for p in leads:
            status     = get_status(p, "Status") or "Sem status"
            nutri      = get_select(p, "Nutricionista") or ""
            origem     = get_select(p, "Origem") or ""
            onde_parou = get_select(p, "Onde parou") or ""
            nome_lead  = get_title(p, "Name")
            data_pc    = get_date(p, "Primeiro contato") or ""

            funil[status] = funil.get(status, 0) + 1

            if status == "Venda":
                vendas_mes += 1
            if status not in ("Venda", "Desistiu"):
                pipeline_ativo += 1

            if in_range(data_pc, inicio_semana, today):
                leads_semana_list.append({
                    "nome":          nome_lead,
                    "data_dd_mm":    fmt_dd_mm(data_pc),
                    "nutricionista": nutri,
                    "origem":        origem,
                    "status":        status,
                })
                if status == "Venda":
                    conversoes_semana += 1
                if onde_parou == "Apenas clicou no link":
                    leads_frios_semana += 1

        total_leads_semana  = len(leads_semana_list)
        taxa_conv_semana    = pct(conversoes_semana, total_leads_semana)
        taxa_conv_mes       = pct(vendas_mes, total_leads_mes)
        print(f"  Leads mês: {total_leads_mes} | Semana: {total_leads_semana} | Frios: {leads_frios_semana}")
    except Exception as e:
        print(f"  [ERRO] CRM Leads: {e}")
        total_leads_semana = taxa_conv_semana = taxa_conv_mes = 0
        vendas_mes = pipeline_ativo = 0

    # ---- 4. Renovações (mês) ----
    print("[4/4] Buscando renovações...")
    renovacoes_semana_list = []
    pipeline_ren_list      = []
    renovaram_mes          = 0
    antecipadas_mes        = 0
    confirmadas_mes        = 0
    perdidas_mes           = 0
    em_aberto_mes          = 0
    total_ren_mes          = 0
    previstas_semana       = 0
    confirmadas_semana     = 0
    perdidas_semana        = 0
    try:
        renovacoes = notion_query(RENOVACOES_DB, filter_obj={"and":[
            {"property":"Data","date":{"on_or_after": str(mes_inicio)}},
            {"property":"Data","date":{"on_or_before":str(mes_fim)}},
        ]})
        total_ren_mes = len(renovacoes)
        for p in renovacoes:
            nome_ren = get_title(p, "Nome")
            data_ren = get_date(p, "Data") or ""
            resp_ren = get_select(p, "Responsável") or ""
            renovou  = get_status(p, "Renovou") or ""
            consult  = get_status(p, "Consultoria") or ""

            # Totais do mês
            if renovou in ("Renovação", "Renovação Antecipada"):
                renovaram_mes += 1
                if renovou == "Renovação Antecipada":
                    antecipadas_mes += 1
                else:
                    confirmadas_mes += 1
            elif renovou == "Não renovou":
                perdidas_mes += 1
            elif renovou in ("Em andamento", "Mensagem enviada", "Entrar em contato"):
                em_aberto_mes += 1

            # Semana
            if in_range(data_ren, inicio_semana, today):
                previstas_semana += 1
                ren_item = {
                    "nome":        nome_ren,
                    "data_dd_mm":  fmt_dd_mm(data_ren),
                    "responsavel": resp_ren,
                    "consultoria": consult,
                    "status":      renovou,
                }
                renovacoes_semana_list.append(ren_item)
                if renovou in ("Renovação", "Renovação Antecipada"):
                    confirmadas_semana += 1
                elif renovou == "Não renovou":
                    perdidas_semana += 1

            # Pipeline ativo (a partir de amanhã)
            amanha = str(today + timedelta(days=1))
            if (in_range(data_ren, amanha, mes_fim) and
                    renovou in ("Em andamento", "Mensagem enviada")):
                pipeline_ren_list.append({
                    "nome":        nome_ren,
                    "data_dd_mm":  fmt_dd_mm(data_ren),
                    "responsavel": resp_ren,
                    "consultoria": consult,
                    "status":      renovou,
                })

        taxa_renovacao = pct(renovaram_mes, total_ren_mes)
        print(f"  Renovações mês: {total_ren_mes} | Renovaram: {renovaram_mes} | Em aberto: {em_aberto_mes}")
    except Exception as e:
        print(f"  [ERRO] Renovações: {e}")
        taxa_renovacao = 0.0

    # ---- Textos de análise ----
    leit_fat = leitura_faturamento(
        fat_total_mes, meta_total,
        fat_mes["Lucas"], metas["Lucas"],
        fat_mes["AURA"],  metas["AURA"],
        fat_mes["Luanna"],metas["Luanna"],
    )
    leit_leads = leitura_leads(
        total_leads_semana, conversoes_semana, leads_frios_semana,
        taxa_conv_semana, total_leads_mes, taxa_conv_mes,
    )
    leit_ren = leitura_renovacoes(renovaram_mes, perdidas_mes, em_aberto_mes,
                                   total_ren_mes, taxa_renovacao)
    leit_funil = leitura_funil(total_leads_mes, vendas_mes, pipeline_ativo, funil)
    c1, c2, c3 = conclusao(fat_total_mes, meta_total, total_leads_semana,
                            conversoes_semana, renovaram_mes, total_ren_mes, em_aberto_mes)

    # ---- Construção dos blocos Notion ----
    ini     = inicio_semana.strftime("%d/%m")
    fim_sem = today.strftime("%d/%m")
    fim_mes = mes_fim.strftime("%d/%m")

    pct_lucas  = pct(fat_mes["Lucas"],  metas["Lucas"])
    pct_aura   = pct(fat_mes["AURA"],   metas["AURA"])
    pct_luanna = pct(fat_mes["Luanna"], metas["Luanna"])
    pct_total  = pct(fat_total_mes, meta_total)
    status_meta = "✅" if pct_total >= 90 else ("⚠️" if pct_total >= 60 else "🔴")

    blocks = []

    # === FATURAMENTO ===
    blocks.append(callout("💰", [
        rt(f"Faturamento Mensal Acumulado — {mes_nome} {today.year}\n", bold=True),
        rt("Meta do mês: "), rt(fmt_brl(meta_total), bold=True),
        rt(" · Total acumulado: "),
        rt(fmt_brl(fat_total_mes), bold=True, color="green"),
        rt(f" — {pct_total}% da meta {status_meta}", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("Lucas (Premium): ", bold=True),
        rt(fmt_brl(fat_mes["Lucas"]), color="blue"),
        rt(f" / {fmt_brl(metas['Lucas'])} → "), rt(f"{pct_lucas}%", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("AURA (Comfort): ", bold=True),
        rt(fmt_brl(fat_mes["AURA"]), color="yellow"),
        rt(f" / {fmt_brl(metas['AURA'])} → "), rt(f"{pct_aura}%", bold=True),
    ]))
    blocks.append(bullet_block([
        rt("Luanna (Premium): ", bold=True),
        rt(fmt_brl(fat_mes["Luanna"]), color="pink"),
        rt(f" / {fmt_brl(metas['Luanna'])} → "), rt(f"{pct_luanna}%", bold=True),
    ]))
    blocks.append(para([rt("Leitura: "), rt(leit_fat)]))
    blocks.append(divider())

    # === VENDAS DA SEMANA ===
    blocks.append(callout("📋", [
        rt(f"Vendas da Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{len(vendas_semana_list)} venda(s) registrada(s) · Faturamento bruto: "),
        rt(fmt_brl(fat_semana), bold=True, color="green"),
    ]))
    if vendas_semana_list:
        for v in vendas_semana_list:
            cor = OWNER_COLORS.get(v["responsavel"], "gray")
            blocks.append(bullet_block([
                rt(fmt_brl(v["valor"]), bold=True, color="green"),
                rt(f" ({v['carater']}, {v['forma_pag']}) — {v['nome']} — responsável: "),
                rt(v["responsavel"], bold=True, color=cor),
                rt(f" · {v['produto']} · {v['data_dd_mm']}"),
            ]))
    else:
        blocks.append(para([rt("Nenhuma venda registrada no período.")]))
    if despesas_semana_list:
        blocks.append(h2("Despesas da Semana"))
        for d in despesas_semana_list:
            blocks.append(bullet_block([
                rt(fmt_brl(d["valor"]), bold=True, color="red"),
                rt(f" ({d['forma_pag']}) — {d['nome']} · {d['data_dd_mm']}"),
            ]))
    else:
        blocks.append(para([rt("Nenhuma despesa registrada no período.")]))
    blocks.append(divider())

    # === CRM DE LEADS ===
    blocks.append(callout("🎯", [
        rt(f"CRM de Leads — Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{total_leads_semana} novo(s) lead(s) na semana · {conversoes_semana} conversão(ões) imediata(s)"),
    ]))
    if leads_semana_list:
        for lead in leads_semana_list:
            cor = OWNER_COLORS.get(lead["nutricionista"], "gray")
            sc  = ("green" if lead["status"] == "Venda"
                   else "red" if lead["status"] == "Desistiu" else None)
            blocks.append(bullet_block([
                rt(f"{lead['nome']} — {lead['data_dd_mm']} — "),
                rt(lead["nutricionista"], bold=True, color=cor),
                rt(f" — {lead['origem']} → "),
                rt(lead["status"], bold=True, color=sc) if sc else rt(lead["status"]),
            ]))
    else:
        blocks.append(para([rt("Nenhum novo lead registrado no período.")]))
    blocks.append(para([rt("Leitura: "), rt(leit_leads)]))
    blocks.append(divider())

    # === RENOVAÇÕES ===
    blocks.append(callout("🔄", [
        rt(f"Renovações — Semana {num_semana} ({ini}–{fim_sem})\n", bold=True),
        rt(f"{previstas_semana} renovação(ões) prevista(s) na semana · "),
        rt(f"{confirmadas_semana} confirmada(s) · {perdidas_semana} não renovou(aram)"),
    ]))
    if renovacoes_semana_list:
        for ren in renovacoes_semana_list:
            cor = OWNER_COLORS.get(ren["responsavel"], "gray")
            sc  = ("green" if "Renovação" in ren["status"]
                   else "red" if "Não renovou" in ren["status"] else None)
            blocks.append(bullet_block([
                rt(f"{ren['nome']} — {ren['data_dd_mm']} — "),
                rt(ren["responsavel"], bold=True, color=cor),
                rt(f" — {ren['consultoria']} → "),
                rt(ren["status"], bold=True, color=sc) if sc else rt(ren["status"]),
            ]))
    else:
        blocks.append(para([rt("Nenhuma renovação prevista para esta semana.")]))

    blocks.append(h2(f"Pipeline Ativo — Vencendo até {fim_mes}"))
    if pipeline_ren_list:
        for p_item in pipeline_ren_list:
            cor = OWNER_COLORS.get(p_item["responsavel"], "gray")
            blocks.append(bullet_block([
                rt(f"{p_item['nome']} — {p_item['data_dd_mm']} — "),
                rt(p_item["responsavel"], bold=True, color=cor),
                rt(f" — {p_item['consultoria']} → {p_item['status']}"),
            ]))
    else:
        blocks.append(para([rt("Nenhuma renovação em aberto até o fim do mês.")]))
    blocks.append(para([rt("Leitura: "), rt(leit_ren)]))
    blocks.append(divider())

    # === FUNIL DO MÊS ===
    STATUS_ORDER = ["Venda","Qualificação","Proposta feita","Novo",
                    "Desistiu","Negociação","Link enviado"]
    blocks.append(callout("📊", [
        rt(f"Funil do Mês — {mes_nome} {today.year}\n", bold=True),
        rt(f"{total_leads_mes} leads no mês · Taxa de conversão: "),
        rt(f"{taxa_conv_mes}%", bold=True),
        rt(f" ({vendas_mes}/{total_leads_mes}) · Pipeline ativo: "),
        rt(f"{pipeline_ativo} leads", bold=True),
    ]))
    for s in STATUS_ORDER:
        n = funil.get(s, 0)
        if s == "Venda":
            blocks.append(bullet_block([rt("✅ Venda: ", bold=True, color="green"),
                                        rt(str(n), bold=True, color="green")]))
        elif s == "Desistiu":
            blocks.append(bullet_block([rt("Desistiu: ", bold=True, color="red"),
                                        rt(str(n), bold=True, color="red")]))
        else:
            blocks.append(bullet_block([rt(f"{s}: {n}")]))
    # FUPs (qualquer status contendo "FUP" ou "Follow")
    fup_total = sum(v for k, v in funil.items()
                    if k not in STATUS_ORDER)
    if fup_total > 0:
        blocks.append(bullet_block([rt(f"Outros / FUP: {fup_total}")]))
    blocks.append(para([rt("Leitura: "), rt(leit_funil)]))
    blocks.append(divider())

    # === CONCLUSÃO EXECUTIVA ===
    blocks.append(h2("Conclusão Executiva"))
    blocks.append(para([rt(c1)]))
    blocks.append(para([rt(c2)]))
    blocks.append(para([rt(c3)]))
    blocks.append(para([rt("")]))
    blocks.append(para([rt("Próximos passos:", bold=True)]))
    proximos = [
        f"Acompanhar os {em_aberto_mes} lead(s) de renovação em aberto até {fim_mes}." if em_aberto_mes > 0 else "Monitorar pipeline de renovações para o próximo mês.",
        f"Focar em converter os {pipeline_ativo} lead(s) ativos no CRM." if pipeline_ativo > 0 else "Intensificar captação de novos leads.",
        f"Revisar estratégia para os {leads_frios_semana} lead(s) frio(s) da semana." if leads_frios_semana > 0 else "Manter qualidade na abordagem de novos leads.",
    ]
    for p_txt in proximos:
        blocks.append(bullet_block([rt(p_txt)]))

    # ---- Criar página Notion ----
    print("\nCriando página no Notion...")
    page_url = ""
    try:
        page     = notion_create_page(ERP_PARENT_PAGE, titulo)
        page_id  = page["id"]
        page_url = page.get("url", "")
        notion_append(page_id, blocks)
        print(f"  Página criada: {page_url}")
    except Exception as e:
        print(f"  [ERRO] Página: {e}")

    # ---- Slack ----
    print("Enviando notificação Slack...")
    slack_payload = {"blocks": [
        {"type":"header","text":{"type":"plain_text","text":f"📊 {titulo}"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":f"📅 *SEMANA ({ini}–{fim_sem})*"}},
        {"type":"section","text":{"type":"mrkdwn","text":(
            f"• *Faturamento:* {fmt_brl(fat_semana)}\n"
            f"• *Novos leads:* {total_leads_semana}  |  Vendas: {conversoes_semana}  |  Conversão: {taxa_conv_semana}%\n"
            f"   ↳ Leads frios (apenas clicaram no link): {leads_frios_semana}\n"
            f"• *Renovações:* {previstas_semana} prevista(s) na semana\n"
            f"   ↳ Previstas: {previstas_semana}  |  Confirmadas: {confirmadas_semana}  |  Perdidas: {perdidas_semana}"
        )}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":f"📆 *{mes_nome} — ACUMULADO*"}},
        {"type":"section","text":{"type":"mrkdwn","text":(
            f"• *Faturamento:* {fmt_brl(fat_total_mes)} — {pct_total}% da meta\n"
            f"   ↳ Lucas: {pct_lucas}%  |  AURA: {pct_aura}%  |  Luanna: {pct_luanna}%\n"
            f"• *Leads no mês:* {total_leads_mes}  |  Vendas: {vendas_mes}  |  Conversão: {taxa_conv_mes}%\n"
            f"• *Renovações:* {renovaram_mes}/{total_ren_mes} — {taxa_renovacao}% de taxa de renovação\n"
            f"   ↳ Antecipadas: {antecipadas_mes}  |  Confirmadas: {confirmadas_mes}  |  Perdidas: {perdidas_mes}  |  Em aberto: {em_aberto_mes}"
        )}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":(
            f"<{page_url}|Ver relatório no Notion>" if page_url else "Relatório criado no Notion."
        )}},
    ]}
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=slack_payload)
        print(f"  Slack: {'ok' if resp.text == 'ok' else resp.text}")
    except Exception as e:
        print(f"  [ERRO] Slack: {e}")

    print(f"\n=== Agente Semanal finalizado: {titulo} ===")


def bullet_block(rich_text):
    return {"object":"block","type":"bulleted_list_item",
            "bulleted_list_item":{"rich_text":rich_text}}


if __name__ == "__main__":
    main()
