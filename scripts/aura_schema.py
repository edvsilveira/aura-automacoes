#!/usr/bin/env python3
"""
Diagnóstico: imprime os campos e valores de exemplo de cada base do Notion.
Rodar uma vez para mapear as propriedades antes de reescrever o agente semanal.
"""

import os
import json
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

DATABASES = {
    "Balanço Geral":        "238f915e8d4f80a7ae63e166f6f3e50f",
    "Metas de Faturamento": "248f915e8d4f8003bcc7e513490b0603",
    "Renovações":           "245f915e8d4f803e9a7cc220338cb568",
    "CRM Leads":            "2e7f915e8d4f807e8fb4fa615f02bd15",
}


def query_one(db_id):
    r = requests.post(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        headers=NOTION_HEADERS,
        json={"page_size": 3},
    )
    r.raise_for_status()
    return r.json().get("results", [])


def extract_value(prop):
    ptype = prop.get("type", "")
    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    if ptype == "date":
        d = prop.get("date") or {}
        return d.get("start")
    if ptype == "number":
        return prop.get("number")
    if ptype == "select":
        s = prop.get("select") or {}
        return s.get("name")
    if ptype == "multi_select":
        return [s.get("name") for s in prop.get("multi_select", [])]
    if ptype == "checkbox":
        return prop.get("checkbox")
    if ptype == "formula":
        f = prop.get("formula") or {}
        return f.get("number") or f.get("string") or f.get("boolean")
    if ptype == "url":
        return prop.get("url")
    if ptype == "email":
        return prop.get("email")
    if ptype == "phone_number":
        return prop.get("phone_number")
    if ptype == "relation":
        return f"[{len(prop.get('relation', []))} relações]"
    if ptype == "rollup":
        ru = prop.get("rollup") or {}
        return ru.get("number") or ru.get("array")
    return f"(tipo: {ptype})"


for name, db_id in DATABASES.items():
    print(f"\n{'='*60}")
    print(f"BASE: {name}")
    print(f"ID:   {db_id}")
    print(f"{'='*60}")
    try:
        pages = query_one(db_id)
        if not pages:
            print("  (sem registros)")
            continue
        for i, page in enumerate(pages):
            print(f"\n  --- Registro {i+1} ---")
            for key, val in page.get("properties", {}).items():
                value = extract_value(val)
                ptype = val.get("type", "")
                print(f"  [{ptype:15}] {key}: {value}")
    except Exception as e:
        print(f"  [ERRO] {e}")
