import json

with open("json/core_types_all_pages.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Converte para chaves numéricas (int)
type_id_map = {int(item["id"]): item["code"] for item in data["data"]}

# Salva como arquivo Python direto (com chaves inteiras)
with open("core_type_ids.py", "w", encoding="utf-8") as f:
    f.write("core_type_id_to_name = {\n")
    for k, v in type_id_map.items():
        f.write(f"    {k}: \"{v}\",\n")
    f.write("}\n")

print("✅ core_type_ids.py criado com chaves numéricas.")
