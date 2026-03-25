import chardet
import re
from collections import defaultdict

# =========================================================
# CONFIGURAÇÕES
# =========================================================

ARQUIVO_DUMP = "C:/Users/esteio/Desktop/query_metadata/dump_utf8.sql"
ARQUIVO_INICIAL = "C:/Users/esteio/Desktop/sql_files/dados_brutos_exportados.sql"
ARQUIVO_NORMALIZADO = "C:/Users/esteio/Desktop/sql_files/sql_normalizado_V2.sql"
LOG = "process.log"

# =========================================================
# UTIL
# =========================================================

def escreve_log(msg):
    print(msg)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# =========================================================
# ENCODING
# =========================================================

def verificar_encoding(caminho):
    with open(caminho, "rb") as f:
        raw = f.read()

    encoding = chardet.detect(raw).get("encoding")
    escreve_log(f"[INFO] Encoding detectado: {encoding}")
    return encoding

<<<<<<< HEAD
    # BLOB (CORREÇÃO PRINCIPAL)
    elif hasattr(valor, "read"):
        try:
            blob_data = valor.read()

            if not blob_data:
                return "NULL"

            # tenta converter para texto
            try:
                texto = blob_data.decode("utf-8")
            except:
                texto = blob_data.decode("latin1", errors="ignore")

            texto = texto.replace("\\", "\\\\")
            texto = texto.replace("'", "''")

            return f"'{texto}'"

        except:
            return "NULL"
        
        # String normal
    elif isinstance(valor, str):
        try:
            valor = valor.encode("latin1").decode("utf-8")
        except:
            valor = valor.encode("utf-8", errors="ignore").decode("utf-8")

        valor = valor.replace("\\", "\\\\")
        valor = valor.replace("'", "''")

        return f"'{valor}'"

    # Boolean
    elif isinstance(valor, bool):
        return '1' if valor else '0'

    # bytes soltos (tratar como texto)
    elif isinstance(valor, (bytes, bytearray)):
        try:
            texto = valor.decode("utf-8")
        except:
            texto = valor.decode("latin1", errors="ignore")

        texto = texto.replace("\\", "\\\\")
        texto = texto.replace("'", "''")

        return f"'{texto}'"
    return str(valor)
=======
def ler_sql(caminho):
    encoding = verificar_encoding(caminho)
    with open(caminho, "r", encoding=encoding, errors="ignore") as f:
        return f.read()
>>>>>>> parent of 4b8dbda (Unificação da conversão em FDB e correção da SQL)

# =========================================================
# FK
# =========================================================

def extrair_lista_fk(caminho_dump):
    pattern = re.compile(
        r"ALTER TABLE (\w+)\s+ADD CONSTRAINT (\w+).*?REFERENCES (\w+)",
        re.IGNORECASE | re.DOTALL
    )

    with open(caminho_dump, "r", encoding="utf-8") as f:
        conteudo = f.read()

    lista = []
    for t, c, ref in pattern.findall(conteudo):
        lista.append((t.upper(), c, ref.upper()))

    return lista

def montar_grafo(lista_fks):
    grafo = defaultdict(set)

    for tabela, _, ref in lista_fks:
        if tabela != ref:
            grafo[tabela].add(ref)

        if ref not in grafo:
            grafo[ref] = set()

<<<<<<< HEAD
        # Desativa checagem de Foreign Key no MySQL
        f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
=======
    return grafo
>>>>>>> parent of 4b8dbda (Unificação da conversão em FDB e correção da SQL)

def ordenar_tabelas(grafo):
    visitado = set()
    resultado = []

    def dfs(t):
        if t in visitado:
            return
        visitado.add(t)

        for dep in grafo[t]:
            dfs(dep)

        resultado.append(t)

    for t in grafo:
        dfs(t)

    return resultado

def gerar_ordem_dependencia():
    escreve_log("[STEP] Extraindo FKs...")
    fks = extrair_lista_fk(ARQUIVO_DUMP)

    escreve_log(f"[INFO] Total de FKs: {len(fks)}")

    grafo = montar_grafo(fks)
    ordem = ordenar_tabelas(grafo)

    print("\n===== ORDEM DE INSERÇÃO =====\n")
    print(" -> ".join(ordem))
    print("\n=============================\n")

    return ordem

# =========================================================
# PROTEGER STRINGS
# =========================================================

def proteger_strings(sql):
    mapa = {}

    def replacer(match):
        chave = f"__STR_{len(mapa)}__"
        mapa[chave] = match.group(0)
        return chave

    sql = re.sub(r"'([^']|'')*'", replacer, sql)
    return sql, mapa

def restaurar_strings(sql, mapa):
    def replacer(match):
        return mapa.get(match.group(0), match.group(0))

    return re.sub(r"__STR_\d+__", replacer, sql)

# =========================================================
# NORMALIZAÇÃO
# =========================================================

def normalize_types(sql):
    escreve_log("[STEP] Normalizando tipos")

    sql = re.sub(r"\bSTRING(\d+)\b", r"VARCHAR(\1)", sql, flags=re.IGNORECASE)
    sql = sql.replace('"DATA"', '`DATA`')
    sql = sql.replace('"DOUBLE"', '"DECIMAL')

    return sql

def limpar_sql(sql):
    escreve_log("[STEP] Limpando SQL")

    sql = re.sub(r"[^\x09\x0A\x0D\x20-\x7EÀ-ÿ]", "", sql)
    sql = re.sub(r"[ \t]+", " ", sql)

    return sql

def corrigir_strings(sql):
    escreve_log("[STEP] Corrigindo strings quebradas")

    while re.search(r"'([^']*?)\n([^']*?)'", sql, re.S):
        sql = re.sub(r"'([^']*?)\n([^']*?)'", r"'\1 \2'", sql)

    return sql

# =========================================================
# EXTRAIR INSERTS (ROBUSTO)
# =========================================================

def extrair_inserts(sql):
    inserts = defaultdict(list)

    atual = []
    dentro_string = False
    i = 0

    while i < len(sql):
        c = sql[i]

        if c == "'":
            if dentro_string and i + 1 < len(sql) and sql[i + 1] == "'":
                atual.append("''")
                i += 2
                continue
            dentro_string = not dentro_string

        if c == ";" and not dentro_string:
            comando = "".join(atual).strip()

            if comando.upper().startswith("INSERT INTO"):
                match = re.search(r'INSERT INTO (\w+)', comando, re.IGNORECASE)
                if match:
                    tabela = match.group(1).upper()
                    inserts[tabela].append(comando + ";")

            atual = []
        else:
            atual.append(c)

        i += 1

    return inserts

# =========================================================
# ORDENAR SQL
# =========================================================

def gerar_sql_ordenado(ordem, sql):
    escreve_log("[STEP] Ordenando INSERTs")

    inserts = extrair_inserts(sql)
    resultado = []

    for tabela in ordem:
        if tabela in inserts:
            escreve_log(f"[INFO] {tabela}")
            resultado.extend(inserts[tabela])

    # extras
    for tabela in inserts:
        if tabela not in ordem:
            resultado.extend(inserts[tabela])

    return "\n".join(resultado)

# =========================================================
# FK CHECK
# =========================================================

def aplicar_fk_check(sql):
    return "SET FOREIGN_KEY_CHECKS=0;\n\n" + sql + "\n\nSET FOREIGN_KEY_CHECKS=1;"

# =========================================================
# SALVAR
# =========================================================

def salvar_sql(caminho, conteudo):
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(conteudo)

# =========================================================
# MAIN
# =========================================================

def process_sql():
    escreve_log("========== INICIO ==========")

    ordem = gerar_ordem_dependencia()

    sql = ler_sql(ARQUIVO_INICIAL)

    #  protege strings
    sql, mapa = proteger_strings(sql)

    # limpa fora de string
    sql = normalize_types(sql)
    sql = limpar_sql(sql)

    # restaura
    sql = restaurar_strings(sql, mapa)

    # ordena inserts
    sql_final = gerar_sql_ordenado(ordem, sql)

    # FK check
    sql_final = aplicar_fk_check(sql_final)

    salvar_sql(ARQUIVO_NORMALIZADO, sql_final)

    escreve_log("[FINALIZADO]")

# =========================================================


# Executa a main
if __name__ == "__main__":
    process_sql()